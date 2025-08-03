@echo off
echo ================================================
echo ПЕРЕКЛЮЧЕНИЕ НА GITHUB РЕПОЗИТОРИЙ
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
echo 2. Переключаем remote на GitHub...
git remote set-url origin https://github.com/albud1978/Helicomponents.git
if %ERRORLEVEL% NEQ 0 (
    echo ОШИБКА: Не удалось изменить remote origin
    pause
    exit /b 1
)

echo.
echo 3. Проверяем новый remote...
git remote get-url origin

echo.
echo 4. Выполняем push в GitHub...
git push -u origin master
if %ERRORLEVEL% NEQ 0 (
    echo ОШИБКА: Push в GitHub не удался
    echo Попробуйте force push: git push -f origin master
    pause
    exit /b 1
)

echo.
echo ================================================
echo ✅ УСПЕШНО ПЕРЕКЛЮЧЕНО НА GITHUB!
echo Repository: https://github.com/albud1978/Helicomponents
echo ================================================
echo.
pause