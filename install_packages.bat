@echo off
REM Install required packages into ArcGIS Pro Python environment

echo Installing packages into ArcGIS Pro Python environment...
echo.

REM Set the path to ArcGIS Pro Python
set ARCGIS_PYTHON="C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

REM Check if Python exists
if not exist %ARCGIS_PYTHON% (
    echo ERROR: ArcGIS Pro Python not found at %ARCGIS_PYTHON%
    echo Please verify the installation path.
    pause
    exit /b 1
)

echo Found ArcGIS Pro Python at %ARCGIS_PYTHON%
echo.

REM Install packages
echo Installing psycopg2-binary...
%ARCGIS_PYTHON% -m pip install psycopg2-binary

echo.
echo Installing py7zr...
%ARCGIS_PYTHON% -m pip install py7zr

echo.
echo Installing rarfile...
%ARCGIS_PYTHON% -m pip install rarfile

echo.
echo ============================================
echo Installation complete!
echo ============================================
echo.
echo Verifying installations...
%ARCGIS_PYTHON% -c "import arcpy; print('✓ arcpy')"
%ARCGIS_PYTHON% -c "import psycopg2; print('✓ psycopg2')"
%ARCGIS_PYTHON% -c "import py7zr; print('✓ py7zr')"
%ARCGIS_PYTHON% -c "import rarfile; print('✓ rarfile')"

echo.
echo All packages are installed and working!
pause

