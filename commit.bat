@echo off
set today=%date%
git add -A
git commit -m "Data %today%"
git push -u origin testing_page
pause
