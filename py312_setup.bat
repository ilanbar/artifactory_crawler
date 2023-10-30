set PYTHON=c:\Python312\python.exe
%PYTHON% -m venv .venv
call .venv\scripts\activate
pip3 install -r py312_requirements.txt
