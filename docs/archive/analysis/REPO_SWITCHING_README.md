# ПЕРЕКЛЮЧЕНИЕ МЕЖДУ РЕПОЗИТОРИЯМИ

## СКРИПТЫ ПЕРЕКЛЮЧЕНИЯ

### 🔄 switch_to_gitlab.bat
**Назначение:** Переключение с GitHub обратно на GitLab Утэйр
**Что делает:**
- Переключает remote origin на GitLab
- Синхронизируется с GitLab (pull)
- Отправляет изменения в GitLab (push)
- Отключает git pager (решает проблему с less)

### 🔄 switch_to_github.bat  
**Назначение:** Переключение с GitLab на приватный GitHub
**Что делает:**
- Переключает remote origin на GitHub
- Отправляет все изменения в GitHub (push)
- Устанавливает upstream tracking
- Отключает git pager (решает проблему с less)

## ИСПОЛЬЗОВАНИЕ

### Переключение на GitLab:
```cmd
.\tools\git\switch_to_gitlab.bat
```

### Переключение на GitHub:
```cmd
.\tools\git\switch_to_github.bat
```

## ЦЕЛЕВЫЕ РЕПОЗИТОРИИ

**GitLab (корпоративный):**
- URL: https://gitlab.utair.ru/ati-frcst/cube.git
- Доступ: через корпоративные credentials
- Проблема: смена пароля GitLab

**GitHub (приватный):**
- URL: https://github.com/albud1978/Helicomponents.git
- Доступ: через личный аккаунт albud1978
- Преимущества: для AI агентов, стабильный доступ

## РЕШЕННЫЕ ПРОБЛЕМЫ

✅ **Git Pager:** Скрипты отключают less режим  
✅ **Безопасность:** Скрипты исключены из git (.gitignore)  
✅ **Автоматизация:** Полный цикл переключения в одном клике  
✅ **Обработка ошибок:** Проверка каждого шага с информативными сообщениями

## РЕКОМЕНДАЦИИ

1. **Сначала попробуйте GitLab:** `.\tools\git\switch_to_gitlab.bat`
2. **Если проблемы с паролем GitLab:** `.\tools\git\switch_to_github.bat`
3. **Для работы с AI агентами:** используйте GitHub
4. **Для корпоративной синхронизации:** используйте GitLab

**Дата создания:** 31-01-2025  
**Автор:** Helicomponents Project Team