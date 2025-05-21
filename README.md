# freshservice-user-cleanup

Various functions to clean up requesters in bulk using Freshservice API.

# Setup

1. `git clone git@github.com:oliverba-unity/freshservice-user-cleanup.git`
2. `python3 -m venv .venv`
3. Activate the virtual environment
   1. Bash: `source ./venv/bin/activate`
   2. Fish: `source .venv/bin/activate.fish`
4. `pip install -r requirements.txt`
5. `cp .env.example .env`
6. `nano .env`
7. `python3 main.py`