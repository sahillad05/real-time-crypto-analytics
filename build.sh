#!/usr/bin/env bash
# Render build script
# Installs dependencies when deploying

pip install --upgrade pip
pip install -r requirements.txt

# Initialize database tables (safe to run multiple times)
python -c "
from database.connection import init_database, test_connection
if test_connection():
    init_database()
    print('Database tables initialized successfully.')
else:
    print('WARNING: Could not connect to database. Set DATABASE_URL in Render environment.')
"
