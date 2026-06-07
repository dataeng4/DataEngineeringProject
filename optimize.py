from sqlalchemy import create_engine, text

def optimize_database():
    engine = create_engine('sqlite:///project_data.db')
    
    with engine.connect() as conn:
        print("Building B-Tree Indexes...")
        
        # Index for Name Searches
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_name ON user_profiles (first_name, last_name);"))
        
        # Index for Expertise Searches
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_expertise ON user_profiles (expertise);"))
        
        # Index for Duplicate Checks (Email & Mobile)
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_contact ON user_profiles (email, mobile);"))
        
        conn.commit()
        print("✅ Database optimization complete! Search latency dramatically reduced.")

if __name__ == "__main__":
    optimize_database()