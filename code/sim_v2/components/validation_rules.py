"""
ValidationRules - правила валидации симуляции

Ответственность:
- Валидация инвариантов симуляции (размерности, состояния, переходы)
- Проверка бизнес-правил (LL/OH/BR лимиты, квоты)
- Детектирование аномалий (отрицательные значения, невалидные переходы)
- Fail-fast проверки на критичных этапах

Архитектурный принцип:
- Изолированные правила валидации (Single Responsibility)
- Возможность включения/отключения проверок
- Детальные сообщения об ошибках для отладки
- Разделение на уровни: критичные, предупреждения, информационные

Источник:
- docs/validation.md
"""

from typing import Dict, List, Optional, Tuple
from enum import Enum
import pyflamegpu as fg


class ValidationLevel(Enum):
    """Уровни валидации"""
    CRITICAL = "CRITICAL"    # Критичные ошибки, останавливают симуляцию
    WARNING = "WARNING"      # Предупреждения, логируются но не останавливают
    INFO = "INFO"            # Информационные сообщения


class ValidationResult:
    """Результат валидации"""
    
    def __init__(self, passed: bool = True, level: ValidationLevel = ValidationLevel.INFO,
                 message: str = "", rule_name: str = ""):
        self.passed = passed
        self.level = level
        self.message = message
        self.rule_name = rule_name
    
    def __bool__(self):
        return self.passed
    
    def __str__(self):
        status = "✅ PASSED" if self.passed else f"❌ FAILED ({self.level.value})"
        return f"[{self.rule_name}] {status}: {self.message}"


class DimensionValidator:
    """Валидатор размерностей симуляции"""
    
    @staticmethod
    def validate_frames_days(frames: int, days: int) -> ValidationResult:
        """Проверяет корректность размерностей FRAMES и DAYS"""
        if frames <= 0:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"FRAMES должен быть > 0, получено: {frames}",
                rule_name="FRAMES_POSITIVE"
            )
        
        if days <= 0:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"DAYS должен быть > 0, получено: {days}",
                rule_name="DAYS_POSITIVE"
            )
        
        # Предупреждение о больших размерностях (возможные проблемы с памятью)
        if frames * days > 10_000_000:
            return ValidationResult(
                passed=True,
                level=ValidationLevel.WARNING,
                message=f"Большие размерности: FRAMES={frames}, DAYS={days}, total={frames*days}",
                rule_name="DIMENSIONS_LARGE"
            )
        
        return ValidationResult(
            passed=True,
            message=f"Размерности корректны: FRAMES={frames}, DAYS={days}",
            rule_name="DIMENSIONS_OK"
        )
    
    @staticmethod
    def validate_mp5_size(mp5_data: List[int], frames: int, days: int) -> ValidationResult:
        """Проверяет размер данных MP5 с D+1 паддингом"""
        expected = (days + 1) * frames
        actual = len(mp5_data)
        
        if actual < expected:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"MP5 данные короче ожидаемого: {actual} < {expected} ((DAYS+1)*FRAMES)",
                rule_name="MP5_SIZE_INSUFFICIENT"
            )
        
        if actual > expected:
            return ValidationResult(
                passed=True,
                level=ValidationLevel.INFO,
                message=f"MP5 данные длиннее ожидаемого: {actual} > {expected} (будет обрезано)",
                rule_name="MP5_SIZE_EXCESS"
            )
        
        return ValidationResult(
            passed=True,
            message=f"Размер MP5 корректен: {actual} == {expected}",
            rule_name="MP5_SIZE_OK"
        )


class StateTransitionValidator:
    """Валидатор переходов между состояниями"""
    
    # Допустимые переходы: (from_state, to_state)
    ALLOWED_TRANSITIONS = {
        (1, 1),  # inactive → inactive
        (2, 2),  # operations → operations
        (2, 3),  # operations → serviceable
        (2, 4),  # operations → repair
        (2, 6),  # operations → storage
        (3, 3),  # serviceable → serviceable
        (3, 2),  # serviceable → operations (квота)
        (4, 4),  # repair → repair
        (4, 5),  # repair → reserve
        (5, 5),  # reserve → reserve
        (5, 2),  # reserve → operations (квота)
        (6, 6),  # storage → storage (вечный)
    }
    
    @staticmethod
    def validate_transition(from_state: int, to_state: int, 
                           context: Optional[Dict] = None) -> ValidationResult:
        """
        Проверяет допустимость перехода между состояниями
        
        Args:
            from_state: исходное состояние (1-6)
            to_state: целевое состояние (1-6)
            context: контекст перехода (опционально, для детальной проверки)
        """
        if (from_state, to_state) not in StateTransitionValidator.ALLOWED_TRANSITIONS:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"Недопустимый переход: {from_state} → {to_state}",
                rule_name="TRANSITION_INVALID"
            )
        
        # Детальная проверка условий перехода (если контекст предоставлен)
        if context:
            if from_state == 2 and to_state == 4:  # operations → repair
                sne = context.get('sne', 0)
                ppr = context.get('ppr', 0)
                oh = context.get('oh', 0)
                br = context.get('br', 0)
                dn = context.get('dn', 0)
                
                p_next = ppr + dn
                s_next = sne + dn
                
                if not (p_next >= oh and s_next < br):
                    return ValidationResult(
                        passed=False,
                        level=ValidationLevel.WARNING,
                        message=f"Переход 2→4 нарушает условия: p_next={p_next} >= oh={oh} AND s_next={s_next} < br={br}",
                        rule_name="TRANSITION_2_4_CONDITION"
                    )
            
            if from_state == 2 and to_state == 6:  # operations → storage
                sne = context.get('sne', 0)
                ppr = context.get('ppr', 0)
                ll = context.get('ll', 0)
                oh = context.get('oh', 0)
                br = context.get('br', 0)
                dn = context.get('dn', 0)
                
                s_next = sne + dn
                p_next = ppr + dn
                
                # Условие: sne_next >= ll ИЛИ (ppr_next >= oh AND sne_next >= br)
                condition_met = (s_next >= ll) or (p_next >= oh and s_next >= br)
                
                if not condition_met:
                    return ValidationResult(
                        passed=False,
                        level=ValidationLevel.WARNING,
                        message=f"Переход 2→6 нарушает условия: s_next={s_next} >= ll={ll} OR (p_next={p_next} >= oh={oh} AND s_next={s_next} >= br={br})",
                        rule_name="TRANSITION_2_6_CONDITION"
                    )
        
        return ValidationResult(
            passed=True,
            message=f"Переход {from_state} → {to_state} допустим",
            rule_name="TRANSITION_VALID"
        )


class InvariantValidator:
    """Валидатор инвариантов симуляции"""
    
    @staticmethod
    def validate_s6_immutable(s6_values_before: Dict[int, int], 
                              s6_values_after: Dict[int, int]) -> ValidationResult:
        """
        Проверяет инвариант: sne агентов в storage (6) не изменяется
        
        Args:
            s6_values_before: {idx: sne} до шага
            s6_values_after: {idx: sne} после шага
        """
        violations = []
        for idx, sne_before in s6_values_before.items():
            sne_after = s6_values_after.get(idx)
            if sne_after is not None and sne_after != sne_before:
                violations.append(f"idx={idx}: {sne_before} → {sne_after}")
        
        if violations:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"Инвариант S6 нарушен: sne изменился для агентов в storage: {', '.join(violations[:5])}",
                rule_name="INVARIANT_S6_IMMUTABLE"
            )
        
        return ValidationResult(
            passed=True,
            message=f"Инвариант S6 выполнен для {len(s6_values_before)} агентов",
            rule_name="INVARIANT_S6_OK"
        )
    
    @staticmethod
    def validate_delta_sne_equals_sum_dt(delta_sne: int, sum_dt: int, 
                                         tolerance: int = 0) -> ValidationResult:
        """
        Проверяет инвариант: Δsne == sum(dt) для агентов в operations
        
        Args:
            delta_sne: изменение sne за период
            sum_dt: сумма суточных налётов за период
            tolerance: допустимое отклонение
        """
        diff = abs(delta_sne - sum_dt)
        
        if diff > tolerance:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"Инвариант Δsne=sum(dt) нарушен: Δsne={delta_sne}, sum(dt)={sum_dt}, diff={diff}",
                rule_name="INVARIANT_DELTA_SNE"
            )
        
        return ValidationResult(
            passed=True,
            message=f"Инвариант Δsne=sum(dt) выполнен: {delta_sne} == {sum_dt}",
            rule_name="INVARIANT_DELTA_SNE_OK"
        )


class DataQualityValidator:
    """Валидатор качества данных"""
    
    @staticmethod
    def validate_no_negative_values(values: Dict[str, int], prefix: str = "") -> ValidationResult:
        """Проверяет отсутствие отрицательных значений"""
        negatives = {k: v for k, v in values.items() if v < 0}
        
        if negatives:
            items = ', '.join([f"{k}={v}" for k, v in list(negatives.items())[:5]])
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"{prefix}Обнаружены отрицательные значения: {items}",
                rule_name="NO_NEGATIVE_VALUES"
            )
        
        return ValidationResult(
            passed=True,
            message=f"{prefix}Все значения неотрицательные ({len(values)} проверено)",
            rule_name="NO_NEGATIVE_VALUES_OK"
        )
    
    @staticmethod
    def validate_norms_present(ll: int, oh: int, br: int, context: str = "") -> ValidationResult:
        """Проверяет наличие нормативов LL/OH/BR"""
        missing = []
        if ll <= 0:
            missing.append("LL")
        if oh <= 0:
            missing.append("OH")
        if br <= 0:
            missing.append("BR")
        
        if missing:
            return ValidationResult(
                passed=False,
                level=ValidationLevel.CRITICAL,
                message=f"{context}Отсутствуют нормативы: {', '.join(missing)} (ll={ll}, oh={oh}, br={br})",
                rule_name="NORMS_MISSING"
            )
        
        return ValidationResult(
            passed=True,
            message=f"{context}Нормативы присутствуют: ll={ll}, oh={oh}, br={br}",
            rule_name="NORMS_OK"
        )


class ValidationSuite:
    """
    Набор валидаций для различных этапов симуляции
    
    Использование:
        suite = ValidationSuite()
        result = suite.validate_dimensions(frames=286, days=365)
        if not result:
            print(result)
    """
    
    def __init__(self, strict: bool = False):
        """
        Args:
            strict: если True, предупреждения также считаются ошибками
        """
        self.strict = strict
        self.results: List[ValidationResult] = []
    
    def add_result(self, result: ValidationResult):
        """Добавляет результат в список"""
        self.results.append(result)
        return result
    
    def validate_dimensions(self, frames: int, days: int, mp5_data: Optional[List[int]] = None) -> ValidationResult:
        """Валидация размерностей"""
        result = DimensionValidator.validate_frames_days(frames, days)
        self.add_result(result)
        
        if mp5_data is not None:
            mp5_result = DimensionValidator.validate_mp5_size(mp5_data, frames, days)
            self.add_result(mp5_result)
            if not mp5_result:
                return mp5_result
        
        return result
    
    def has_failures(self, include_warnings: bool = False) -> bool:
        """Проверяет наличие ошибок"""
        for r in self.results:
            if not r.passed and (r.level == ValidationLevel.CRITICAL or 
                                (include_warnings and r.level == ValidationLevel.WARNING)):
                return True
        return False
    
    def get_summary(self) -> str:
        """Возвращает сводку по валидациям"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        critical = sum(1 for r in self.results if not r.passed and r.level == ValidationLevel.CRITICAL)
        warnings = sum(1 for r in self.results if not r.passed and r.level == ValidationLevel.WARNING)
        
        summary = f"Валидации: {passed}/{total} прошли"
        if failed > 0:
            summary += f", {failed} провалились (critical={critical}, warnings={warnings})"
        
        return summary
