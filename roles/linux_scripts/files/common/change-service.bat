:: TEST BAT
@echo off
ECHO Changing startup mode for Workstation service to MANUAL
:: sc config "LanmanWorkstation" start= demand >NUL