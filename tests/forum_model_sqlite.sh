#!/bin/sh
sqlite3 -init tests/forum_model_sqlite.sql tests/test.db << EOF
.quit
EOF