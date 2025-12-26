#!/usr/bin/env bash
# Helper to convert Supabase connection URLs from pooler to direct
# Usage: source this file or run: eval $(./fix_db_url.sh)

if [ -f .env ]; then
  # Read current URL
  CURRENT=$(grep "^SUPABASE_DB_URL=" .env | cut -d= -f2-)
  
  # Replace pooler port (6543) with direct port (5432)
  FIXED=$(echo "$CURRENT" | sed 's/:6543\//:5432\//')
  
  if [ "$CURRENT" != "$FIXED" ]; then
    echo "# Add this to your .env file or export it:"
    echo "export SUPABASE_DB_URL_DIRECT='$FIXED'"
    echo ""
    echo "# Or update .env with:"
    echo "# SUPABASE_DB_URL=$FIXED"
  else
    echo "# URL already using direct connection (port 5432)"
  fi
else
  echo "# No .env file found"
fi
