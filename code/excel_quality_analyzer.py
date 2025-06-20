#!/usr/bin/env python3
"""
Excel Quality Analyzer - Анализатор качества данных Excel
Проект: Helicopter Component Lifecycle Prediction

Инструмент для предварительного анализа качества данных Excel файлов
перед загрузкой в систему прогнозирования жизненного цикла компонентов вертолетов.

Автор: AI Assistant
Версия: 1.0
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timedelta
import sys
import argparse
import json
import warnings
warnings.filterwarnings('ignore')

class ExcelQualityAnalyzer:
    """
    Анализатор качества данных Excel файлов
    
    Проводит комплексный анализ качества данных:
    - Структурный анализ
    - Валидация бизнес-правил
    - Статистический анализ
    - Анализ аномалий
    - Рекомендации по улучшению
    """
    
    def __init__(self, excel_path: str, log_level: str = 'INFO'):
        self.excel_path = Path(excel_path)
        self.logger = self._setup_logging(log_level)
        self.df = None
        self.analysis_results = {
            'file_info': {},
            'structure_analysis': {},
            'data_quality': {},
            'business_rules': {},
            'statistical_analysis': {},
            'anomalies': {},
            'recommendations': [],
            'quality_score': 0
        }
        
    def _setup_logging(self, log_level: str) -> logging.Logger:
        """Настройка логирования"""
        logger = logging.getLogger('ExcelQualityAnalyzer')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def load_excel(self, sheet_name: str = None) -> bool:
        """
        Загрузка Excel файла с обработкой ошибок
        
        Args:
            sheet_name: Название листа (если None, берется первый лист)
            
        Returns:
            bool: Успешность загрузки
        """
        try:
            self.logger.info(f"🔍 Анализ Excel файла: {self.excel_path}")
            
            if not self.excel_path.exists():
                self.logger.error(f"❌ Файл не найден: {self.excel_path}")
                return False
            
            # Определяем размер файла
            file_size = self.excel_path.stat().st_size
            self.analysis_results['file_info'] = {
                'path': str(self.excel_path),
                'size_bytes': file_size,
                'size_mb': round(file_size / (1024 * 1024), 2),
                'modified': datetime.fromtimestamp(self.excel_path.stat().st_mtime).isoformat()
            }
            
            # Загружаем с Arrow backend если возможно
            try:
                self.df = pd.read_excel(
                    self.excel_path,
                    sheet_name=sheet_name,
                    engine='openpyxl',
                    dtype_backend="pyarrow"
                )
                self.logger.info("⚡ Использован Arrow backend для оптимальной производительности")
            except Exception as e:
                self.logger.warning(f"Fallback к стандартному чтению: {e}")
                self.df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
            
            self.logger.info(f"✅ Файл загружен: {len(self.df)} строк, {len(self.df.columns)} столбцов")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки Excel: {e}")
            return False
    
    def analyze_structure(self):
        """Анализ структуры данных"""
        self.logger.info("📊 Анализ структуры данных...")
        
        if self.df is None:
            return
        
        # Базовая информация
        structure = {
            'rows': len(self.df),
            'columns': len(self.df.columns),
            'memory_usage_mb': round(self.df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
            'data_types': {},
            'missing_data': {},
            'duplicate_analysis': {}
        }
        
        # Анализ типов данных
        for col in self.df.columns:
            dtype = str(self.df[col].dtype)
            null_count = self.df[col].isnull().sum()
            null_percentage = (null_count / len(self.df)) * 100
            
            structure['data_types'][col] = {
                'dtype': dtype,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'unique_values': self.df[col].nunique(),
                'memory_usage_bytes': self.df[col].memory_usage(deep=True)
            }
            
            # Детальный анализ пропущенных данных
            if null_count > 0:
                structure['missing_data'][col] = {
                    'count': null_count,
                    'percentage': round(null_percentage, 2),
                    'pattern': self._analyze_missing_pattern(col)
                }
        
        # Анализ дубликатов
        total_duplicates = self.df.duplicated().sum()
        structure['duplicate_analysis'] = {
            'total_duplicates': total_duplicates,
            'duplicate_percentage': round((total_duplicates / len(self.df)) * 100, 2)
        }
        
        # Дубликаты по ключевым полям
        key_fields = ['partno', 'serialno']
        available_keys = [field for field in key_fields if field in self.df.columns]
        
        if len(available_keys) >= 2:
            key_duplicates = self.df.duplicated(subset=available_keys).sum()
            structure['duplicate_analysis']['key_duplicates'] = {
                'fields': available_keys,
                'count': key_duplicates,
                'percentage': round((key_duplicates / len(self.df)) * 100, 2)
            }
        
        self.analysis_results['structure_analysis'] = structure
        self.logger.info(f"✅ Структурный анализ завершен")
    
    def _analyze_missing_pattern(self, column: str) -> str:
        """Анализ паттерна пропущенных данных"""
        missing_mask = self.df[column].isnull()
        
        if missing_mask.sum() == 0:
            return "no_missing"
        elif missing_mask.sum() == len(self.df):
            return "all_missing"
        elif missing_mask.iloc[:100].all():
            return "missing_at_start"
        elif missing_mask.iloc[-100:].all():
            return "missing_at_end"
        else:
            return "scattered"
    
    def analyze_data_quality(self):
        """Анализ качества данных"""
        self.logger.info("🔍 Анализ качества данных...")
        
        if self.df is None:
            return
        
        quality = {
            'critical_fields': {},
            'date_fields': {},
            'numeric_fields': {},
            'text_fields': {},
            'quality_issues': []
        }
        
        # Анализ критичных полей
        critical_fields = ['partno', 'serialno']
        for field in critical_fields:
            if field in self.df.columns:
                field_analysis = self._analyze_critical_field(field)
                quality['critical_fields'][field] = field_analysis
                
                if field_analysis['issues']:
                    quality['quality_issues'].extend(field_analysis['issues'])
        
        # Анализ полей дат
        date_fields = ['mfg_date', 'removal_date', 'target_date', 'oh_at_date', 'repair_date']
        for field in date_fields:
            if field in self.df.columns:
                date_analysis = self._analyze_date_field(field)
                quality['date_fields'][field] = date_analysis
                
                if date_analysis['issues']:
                    quality['quality_issues'].extend(date_analysis['issues'])
        
        # Анализ числовых полей
        numeric_fields = ['oh', 'oh_threshold', 'll', 'sne', 'ppr', 'daily_flight_hours']
        for field in numeric_fields:
            if field in self.df.columns:
                numeric_analysis = self._analyze_numeric_field(field)
                quality['numeric_fields'][field] = numeric_analysis
                
                if numeric_analysis['issues']:
                    quality['quality_issues'].extend(numeric_analysis['issues'])
        
        # Анализ текстовых полей
        text_fields = ['condition', 'location', 'ac_typ', 'owner']
        for field in text_fields:
            if field in self.df.columns:
                text_analysis = self._analyze_text_field(field)
                quality['text_fields'][field] = text_analysis
        
        self.analysis_results['data_quality'] = quality
        self.logger.info(f"✅ Анализ качества данных завершен, найдено {len(quality['quality_issues'])} проблем")
    
    def _analyze_critical_field(self, field: str) -> dict:
        """Анализ критичного поля"""
        series = self.df[field]
        issues = []
        
        # Проверка пустых значений
        null_count = series.isnull().sum()
        empty_count = (series.astype(str).str.strip() == '').sum()
        
        if null_count > 0:
            issues.append(f"КРИТИЧНО: {field} имеет {null_count} пустых (NULL) значений")
        
        if empty_count > 0:
            issues.append(f"КРИТИЧНО: {field} имеет {empty_count} пустых строк")
        
        # Анализ уникальности
        unique_count = series.nunique()
        duplicate_count = len(series) - unique_count
        
        return {
            'total_values': len(series),
            'null_count': null_count,
            'empty_count': empty_count,
            'unique_count': unique_count,
            'duplicate_count': duplicate_count,
            'sample_values': series.dropna().astype(str).head(5).tolist(),
            'issues': issues
        }
    
    def _analyze_date_field(self, field: str) -> dict:
        """Анализ поля даты"""
        series = self.df[field]
        issues = []
        
        # Конвертация в даты
        try:
            dates = pd.to_datetime(series, errors='coerce', dayfirst=True)
            valid_dates = dates.dropna()
            invalid_count = len(series) - len(valid_dates)
            
            analysis = {
                'total_values': len(series),
                'valid_dates': len(valid_dates),
                'invalid_dates': invalid_count,
                'issues': issues
            }
            
            if len(valid_dates) > 0:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                current_date = pd.Timestamp.now()
                
                # Проверки разумности дат
                future_dates = (valid_dates > current_date).sum()
                old_dates = (valid_dates < pd.Timestamp('1980-01-01')).sum()
                
                analysis.update({
                    'min_date': min_date.isoformat(),
                    'max_date': max_date.isoformat(),
                    'date_range_days': (max_date - min_date).days,
                    'future_dates': future_dates,
                    'old_dates': old_dates
                })
                
                if future_dates > 0:
                    issues.append(f"ПРЕДУПРЕЖДЕНИЕ: {field} содержит {future_dates} дат в будущем")
                
                if old_dates > 0:
                    issues.append(f"ПРЕДУПРЕЖДЕНИЕ: {field} содержит {old_dates} дат до 1980 года")
            
            if invalid_count > 0:
                invalid_percentage = (invalid_count / len(series)) * 100
                if invalid_percentage > 20:
                    issues.append(f"ПРОБЛЕМА: {field} содержит {invalid_count} невалидных дат ({invalid_percentage:.1f}%)")
                    
        except Exception as e:
            issues.append(f"ОШИБКА: Не удалось обработать поле дат {field}: {e}")
            analysis = {'total_values': len(series), 'issues': issues}
        
        return analysis
    
    def _analyze_numeric_field(self, field: str) -> dict:
        """Анализ числового поля"""
        series = self.df[field]
        issues = []
        
        # Конвертация в числа
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            valid_numbers = numeric_series.dropna()
            invalid_count = len(series) - len(valid_numbers)
            
            analysis = {
                'total_values': len(series),
                'valid_numbers': len(valid_numbers),
                'invalid_numbers': invalid_count,
                'issues': issues
            }
            
            if len(valid_numbers) > 0:
                # Статистики
                stats = {
                    'min': valid_numbers.min(),
                    'max': valid_numbers.max(),
                    'mean': valid_numbers.mean(),
                    'median': valid_numbers.median(),
                    'std': valid_numbers.std(),
                    'zeros': (valid_numbers == 0).sum(),
                    'negatives': (valid_numbers < 0).sum()
                }
                
                analysis['statistics'] = stats
                
                # Проверки качества
                if stats['negatives'] > 0:
                    if field in ['sne', 'ppr', 'oh', 'll']:  # Поля, которые не должны быть отрицательными
                        issues.append(f"КРИТИЧНО: {field} содержит {stats['negatives']} отрицательных значений")
                    else:
                        issues.append(f"ПРЕДУПРЕЖДЕНИЕ: {field} содержит {stats['negatives']} отрицательных значений")
                
                # Анализ выбросов (значения за пределами 1.5 * IQR)
                Q1 = valid_numbers.quantile(0.25)
                Q3 = valid_numbers.quantile(0.75)
                IQR = Q3 - Q1
                
                outliers = valid_numbers[(valid_numbers < (Q1 - 1.5 * IQR)) | 
                                       (valid_numbers > (Q3 + 1.5 * IQR))]
                
                if len(outliers) > 0:
                    outlier_percentage = (len(outliers) / len(valid_numbers)) * 100
                    analysis['outliers'] = {
                        'count': len(outliers),
                        'percentage': round(outlier_percentage, 2),
                        'examples': outliers.head(3).tolist()
                    }
                    
                    if outlier_percentage > 5:
                        issues.append(f"ПРЕДУПРЕЖДЕНИЕ: {field} содержит {len(outliers)} выбросов ({outlier_percentage:.1f}%)")
            
            if invalid_count > 0:
                invalid_percentage = (invalid_count / len(series)) * 100
                if invalid_percentage > 10:
                    issues.append(f"ПРОБЛЕМА: {field} содержит {invalid_count} нечисловых значений ({invalid_percentage:.1f}%)")
                    
        except Exception as e:
            issues.append(f"ОШИБКА: Не удалось обработать числовое поле {field}: {e}")
            analysis = {'total_values': len(series), 'issues': issues}
        
        return analysis
    
    def _analyze_text_field(self, field: str) -> dict:
        """Анализ текстового поля"""
        series = self.df[field].astype(str)
        
        # Базовая статистика
        non_null_series = series[series != 'nan']
        
        analysis = {
            'total_values': len(series),
            'non_null_values': len(non_null_series),
            'unique_values': non_null_series.nunique(),
            'max_length': non_null_series.str.len().max() if len(non_null_series) > 0 else 0,
            'min_length': non_null_series.str.len().min() if len(non_null_series) > 0 else 0,
            'avg_length': round(non_null_series.str.len().mean(), 2) if len(non_null_series) > 0 else 0,
            'empty_strings': (non_null_series.str.strip() == '').sum(),
            'value_distribution': non_null_series.value_counts().head(10).to_dict()
        }
        
        return analysis
    
    def analyze_business_rules(self):
        """Анализ соответствия бизнес-правилам"""
        self.logger.info("📋 Анализ бизнес-правил...")
        
        if self.df is None:
            return
        
        business_rules = {
            'rule_violations': [],
            'consistency_checks': {}
        }
        
        # Правило 1: Дата снятия не может быть раньше даты изготовления
        if 'mfg_date' in self.df.columns and 'removal_date' in self.df.columns:
            try:
                mfg_dates = pd.to_datetime(self.df['mfg_date'], errors='coerce')
                removal_dates = pd.to_datetime(self.df['removal_date'], errors='coerce')
                
                invalid_sequence = ((removal_dates < mfg_dates) & 
                                  mfg_dates.notna() & removal_dates.notna()).sum()
                
                business_rules['consistency_checks']['date_sequence'] = {
                    'description': 'Дата снятия после даты изготовления',
                    'violations': invalid_sequence,
                    'passed': invalid_sequence == 0
                }
                
                if invalid_sequence > 0:
                    business_rules['rule_violations'].append(
                        f"НАРУШЕНИЕ ЛОГИКИ: {invalid_sequence} случаев снятия раньше изготовления"
                    )
                    
            except Exception as e:
                business_rules['rule_violations'].append(f"ОШИБКА проверки дат: {e}")
        
        # Правило 2: SNE и PPR должны быть неотрицательными
        for field in ['sne', 'ppr']:
            if field in self.df.columns:
                try:
                    numeric_series = pd.to_numeric(self.df[field], errors='coerce')
                    negative_count = (numeric_series < 0).sum()
                    
                    business_rules['consistency_checks'][f'{field}_positive'] = {
                        'description': f'{field.upper()} должен быть неотрицательным',
                        'violations': negative_count,
                        'passed': negative_count == 0
                    }
                    
                    if negative_count > 0:
                        business_rules['rule_violations'].append(
                            f"НАРУШЕНИЕ ПРАВИЛА: {field.upper()} содержит {negative_count} отрицательных значений"
                        )
                        
                except Exception as e:
                    business_rules['rule_violations'].append(f"ОШИБКА проверки {field}: {e}")
        
        # Правило 3: Серийные номера должны быть уникальными в рамках партномера
        if 'partno' in self.df.columns and 'serialno' in self.df.columns:
            try:
                # Группируем по партномеру и проверяем уникальность серийных номеров
                duplicates_within_partno = []
                
                for partno, group in self.df.groupby('partno'):
                    dup_serials = group[group.duplicated(subset=['serialno'], keep=False)]
                    if not dup_serials.empty:
                        duplicates_within_partno.append({
                            'partno': partno,
                            'duplicate_serials': dup_serials['serialno'].tolist()
                        })
                
                business_rules['consistency_checks']['serial_uniqueness'] = {
                    'description': 'Уникальность серийных номеров в рамках партномера',
                    'violations': len(duplicates_within_partno),
                    'passed': len(duplicates_within_partno) == 0,
                    'details': duplicates_within_partno[:5]  # Показываем первые 5
                }
                
                if duplicates_within_partno:
                    business_rules['rule_violations'].append(
                        f"НАРУШЕНИЕ УНИКАЛЬНОСТИ: {len(duplicates_within_partno)} партномеров с дублированными серийными номерами"
                    )
                    
            except Exception as e:
                business_rules['rule_violations'].append(f"ОШИБКА проверки уникальности: {e}")
        
        self.analysis_results['business_rules'] = business_rules
        self.logger.info(f"✅ Анализ бизнес-правил завершен, найдено {len(business_rules['rule_violations'])} нарушений")
    
    def calculate_quality_score(self):
        """Расчет общего балла качества данных"""
        self.logger.info("🎯 Расчет балла качества...")
        
        if self.df is None:
            self.analysis_results['quality_score'] = 0
            return
        
        score = 100.0  # Начинаем со 100%
        
        # Структурные проблемы
        structure = self.analysis_results.get('structure_analysis', {})
        
        # Штрафы за пустые данные в критичных полях
        data_quality = self.analysis_results.get('data_quality', {})
        critical_fields = data_quality.get('critical_fields', {})
        
        for field, analysis in critical_fields.items():
            null_percentage = (analysis['null_count'] / analysis['total_values']) * 100
            empty_percentage = (analysis['empty_count'] / analysis['total_values']) * 100
            
            if null_percentage > 0:
                score -= min(20, null_percentage * 2)  # До 20 баллов за пустые значения
            if empty_percentage > 0:
                score -= min(15, empty_percentage * 1.5)  # До 15 баллов за пустые строки
        
        # Штрафы за дубликаты
        duplicate_percentage = structure.get('duplicate_analysis', {}).get('duplicate_percentage', 0)
        if duplicate_percentage > 0:
            score -= min(15, duplicate_percentage * 3)  # До 15 баллов за дубликаты
        
        # Штрафы за проблемы качества данных
        quality_issues = data_quality.get('quality_issues', [])
        critical_issues = [issue for issue in quality_issues if 'КРИТИЧНО' in issue]
        warning_issues = [issue for issue in quality_issues if 'ПРЕДУПРЕЖДЕНИЕ' in issue]
        
        score -= len(critical_issues) * 5  # По 5 баллов за критичную проблему
        score -= len(warning_issues) * 2   # По 2 балла за предупреждение
        
        # Штрафы за нарушения бизнес-правил
        business_rules = self.analysis_results.get('business_rules', {})
        rule_violations = business_rules.get('rule_violations', [])
        score -= len(rule_violations) * 10  # По 10 баллов за нарушение правила
        
        # Ограничиваем диапазон 0-100
        score = max(0, min(100, score))
        
        self.analysis_results['quality_score'] = round(score, 1)
        self.logger.info(f"🎯 Балл качества: {score:.1f}/100")
    
    def generate_recommendations(self):
        """Генерация рекомендаций по улучшению качества данных"""
        self.logger.info("💡 Генерация рекомендаций...")
        
        recommendations = []
        
        # Рекомендации на основе структурного анализа
        structure = self.analysis_results.get('structure_analysis', {})
        
        # Проблемы с дубликатами
        dup_percentage = structure.get('duplicate_analysis', {}).get('duplicate_percentage', 0)
        if dup_percentage > 0:
            recommendations.append({
                'category': 'Дубликаты',
                'priority': 'HIGH' if dup_percentage > 5 else 'MEDIUM',
                'issue': f"Найдено {dup_percentage:.1f}% дубликатов",
                'recommendation': "Удалить дублированные записи или пересмотреть ключевые поля",
                'action': "Используйте df.drop_duplicates() или проанализируйте причины дублирования"
            })
        
        # Рекомендации на основе качества данных
        data_quality = self.analysis_results.get('data_quality', {})
        
        # Критичные поля
        critical_fields = data_quality.get('critical_fields', {})
        for field, analysis in critical_fields.items():
            if analysis['null_count'] > 0 or analysis['empty_count'] > 0:
                recommendations.append({
                    'category': 'Критичные поля',
                    'priority': 'HIGH',
                    'issue': f"Поле {field} содержит пустые значения",
                    'recommendation': f"Заполнить или исключить записи с пустыми {field}",
                    'action': f"Проверить источник данных для поля {field}"
                })
        
        # Проблемы с датами
        date_fields = data_quality.get('date_fields', {})
        for field, analysis in date_fields.items():
            if analysis.get('invalid_dates', 0) > 0:
                recommendations.append({
                    'category': 'Даты',
                    'priority': 'MEDIUM',
                    'issue': f"Поле {field} содержит невалидные даты",
                    'recommendation': "Исправить формат дат или исключить невалидные записи",
                    'action': f"Использовать pd.to_datetime() с правильными параметрами для {field}"
                })
        
        # Проблемы с числовыми полями
        numeric_fields = data_quality.get('numeric_fields', {})
        for field, analysis in numeric_fields.items():
            if analysis.get('invalid_numbers', 0) > 0:
                recommendations.append({
                    'category': 'Числовые поля',
                    'priority': 'MEDIUM',
                    'issue': f"Поле {field} содержит нечисловые значения",
                    'recommendation': "Исправить или исключить нечисловые значения",
                    'action': f"Использовать pd.to_numeric() с errors='coerce' для {field}"
                })
        
        # Рекомендации на основе бизнес-правил
        business_rules = self.analysis_results.get('business_rules', {})
        rule_violations = business_rules.get('rule_violations', [])
        
        for violation in rule_violations:
            recommendations.append({
                'category': 'Бизнес-правила',
                'priority': 'HIGH',
                'issue': violation,
                'recommendation': "Исправить данные в соответствии с бизнес-логикой",
                'action': "Проверить и исправить исходные данные"
            })
        
        # Общие рекомендации
        quality_score = self.analysis_results.get('quality_score', 0)
        
        if quality_score < 70:
            recommendations.append({
                'category': 'Общее качество',
                'priority': 'HIGH',
                'issue': f"Низкий балл качества данных: {quality_score}/100",
                'recommendation': "Провести комплексную очистку данных перед загрузкой",
                'action': "Исправить критичные проблемы и повторить анализ"
            })
        elif quality_score < 85:
            recommendations.append({
                'category': 'Общее качество',
                'priority': 'MEDIUM',
                'issue': f"Средний балл качества данных: {quality_score}/100",
                'recommendation': "Рассмотреть возможность улучшения качества данных",
                'action': "Исправить основные проблемы для повышения качества"
            })
        
        self.analysis_results['recommendations'] = recommendations
        self.logger.info(f"💡 Сгенерировано {len(recommendations)} рекомендаций")
    
    def run_full_analysis(self, sheet_name: str = None) -> dict:
        """
        Запуск полного анализа качества данных
        
        Args:
            sheet_name: Название листа Excel
            
        Returns:
            dict: Результаты анализа
        """
        self.logger.info("🚀 === ЗАПУСК ПОЛНОГО АНАЛИЗА КАЧЕСТВА EXCEL ===")
        
        # Загрузка данных
        if not self.load_excel(sheet_name):
            return self.analysis_results
        
        # Выполнение всех видов анализа
        self.analyze_structure()
        self.analyze_data_quality()
        self.analyze_business_rules()
        self.calculate_quality_score()
        self.generate_recommendations()
        
        self.logger.info("🎉 === АНАЛИЗ ЗАВЕРШЕН ===")
        return self.analysis_results
    
    def save_report(self, output_path: str = None):
        """Сохранение отчета в JSON файл"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"excel_quality_report_{timestamp}.json"
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.analysis_results, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"📄 Отчет сохранен: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения отчета: {e}")
            return None
    
    def print_summary(self):
        """Вывод краткого резюме анализа"""
        print("\n" + "="*80)
        print("📊 РЕЗЮМЕ АНАЛИЗА КАЧЕСТВА EXCEL ДАННЫХ")
        print("="*80)
        
        # Базовая информация
        file_info = self.analysis_results.get('file_info', {})
        structure = self.analysis_results.get('structure_analysis', {})
        
        print(f"📁 Файл: {file_info.get('path', 'N/A')}")
        print(f"📏 Размер: {file_info.get('size_mb', 0)} МБ")
        print(f"📊 Структура: {structure.get('rows', 0):,} строк × {structure.get('columns', 0)} столбцов")
        
        # Балл качества
        quality_score = self.analysis_results.get('quality_score', 0)
        if quality_score >= 85:
            status = "🟢 ОТЛИЧНОЕ"
        elif quality_score >= 70:
            status = "🟡 ХОРОШЕЕ"
        elif quality_score >= 50:
            status = "🟠 СРЕДНЕЕ"
        else:
            status = "🔴 НИЗКОЕ"
        
        print(f"🎯 Качество данных: {quality_score}/100 - {status}")
        
        # Проблемы
        data_quality = self.analysis_results.get('data_quality', {})
        business_rules = self.analysis_results.get('business_rules', {})
        
        quality_issues_count = len(data_quality.get('quality_issues', []))
        rule_violations_count = len(business_rules.get('rule_violations', []))
        
        print(f"⚠️  Проблемы качества: {quality_issues_count}")
        print(f"🚨 Нарушения правил: {rule_violations_count}")
        
        # Рекомендации
        recommendations = self.analysis_results.get('recommendations', [])
        high_priority = len([r for r in recommendations if r.get('priority') == 'HIGH'])
        medium_priority = len([r for r in recommendations if r.get('priority') == 'MEDIUM'])
        
        print(f"💡 Рекомендации: {high_priority} критичных, {medium_priority} средних")
        
        # Топ проблемы
        if quality_issues_count > 0:
            print(f"\n🔍 ТОП ПРОБЛЕМЫ:")
            for i, issue in enumerate(data_quality.get('quality_issues', [])[:3], 1):
                print(f"   {i}. {issue}")
        
        if rule_violations_count > 0:
            print(f"\n🚨 НАРУШЕНИЯ ПРАВИЛ:")
            for i, violation in enumerate(business_rules.get('rule_violations', [])[:3], 1):
                print(f"   {i}. {violation}")
        
        print("="*80)


def main():
    """Основная функция для запуска из командной строки"""
    parser = argparse.ArgumentParser(description='Анализатор качества Excel данных')
    parser.add_argument('excel_file', help='Путь к Excel файлу')
    parser.add_argument('--sheet', help='Название листа (по умолчанию первый лист)')
    parser.add_argument('--output', help='Путь для сохранения отчета JSON')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Уровень логирования')
    
    args = parser.parse_args()
    
    try:
        # Создаем анализатор
        analyzer = ExcelQualityAnalyzer(args.excel_file, args.log_level)
        
        # Запускаем анализ
        results = analyzer.run_full_analysis(args.sheet)
        
        # Выводим резюме
        analyzer.print_summary()
        
        # Сохраняем отчет
        if args.output:
            analyzer.save_report(args.output)
        
        # Возвращаем код выхода на основе качества
        quality_score = results.get('quality_score', 0)
        
        if quality_score >= 85:
            sys.exit(0)  # Отличное качество
        elif quality_score >= 70:
            sys.exit(1)  # Хорошее качество, есть замечания
        elif quality_score >= 50:
            sys.exit(2)  # Среднее качество, нужны исправления
        else:
            sys.exit(3)  # Низкое качество, критичные проблемы
        
    except Exception as e:
        print(f"❌ Ошибка выполнения анализа: {e}")
        sys.exit(4)


if __name__ == '__main__':
    main() 