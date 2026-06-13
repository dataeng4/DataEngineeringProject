from sqlalchemy import text
from database import get_engine

def optimize_database():
    # Dynamically grab whichever engine is active in the .env file
    engine = get_engine()
    
    with engine.begin() as conn:
        print(f"Building B-Tree Indexes on {engine.name.upper()}...")
        
        # Index for Name Searches
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_name ON user_profiles (first_name, last_name);"))
        
        # Index for Expertise Searches
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_expertise ON user_profiles (expertise);"))
        
        # Index for Duplicate Checks (Email & Mobile)
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_contact ON user_profiles (email, mobile);"))
        
        print("✅ Database optimization complete! Search latency dramatically reduced.")

if __name__ == "__main__":
    optimize_database()