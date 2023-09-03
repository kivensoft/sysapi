@echo off
rem 目录定时同步脚本
set project=sysapi
set src=/f/%project%/
set dst=/d/develop/rust/%project%/

rsync -av %dst% %src%
