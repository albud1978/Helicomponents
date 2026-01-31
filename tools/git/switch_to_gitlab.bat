@echo off
echo ================================================
echo ПЕРЕКЛЮЧЕНИЕ НА GITLAB РЕПОЗИТОРИЙ
echo ================================================
echo.

REM Отключаем pager для избежания проблем с less
set GIT_PAGER=
git config --global core.pager ""

echo 1. Проверяем текущий статус...
git status --porcelain
if %ERRORLEVEL% NEQ 0 (
    echo ОШИБКА: Проблемы с git статусом
    pause
    exit /b 1
)

echo.
echo 2. Переключаем remote на GitLab...
git remote set-url origin https://gitlab.utair.ru/ati-frcst/cube.git
if %ERRORLEVEL% NEQ 0 (
    echo ОШИБКА: Не удалось изменить remote origin
    pause
    exit /b 1
)

echo.
echo 3. Проверяем новый remote...
git remote get-url origin

echo.
echo 4. Синхронизируемся с GitLab...
git pull origin master
if %ERRORLEVEL% NEQ 0 (
    echo ПРЕДУПРЕЖДЕНИЕ: Pull не удался, возможны конфликты
    echo Продолжаем с push...
)

echo.
echo 5. Выполняем push в GitLab...
git push origin master
if %ERRORLEVEL% NEQ 0 (
    echo ОШИБКА: Push в GitLab не удался
    echo Возможно нужен новый пароль GitLab
    pause
    exit /b 1
)

echo.
echo ================================================
echo ✅ УСПЕШНО ПЕРЕКЛЮЧЕНО НА GITLAB!
echo Repository: https://gitlab.utair.ru/ati-frcst/cube
echo ================================================
echo.
pause