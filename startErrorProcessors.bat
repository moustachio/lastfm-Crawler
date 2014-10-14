set d="C:\Users\jlorince\Dropbox\Research\PROJECTS\Tagging\lastfm-Crawler\"

cd %d%
python recovery.py

start /D %d% cmd /k python errorProcessor.py
start /D %d% cmd /k python errorProcessorSecondary.py
start /D %d% cmd /k python errorProcessor3.py

