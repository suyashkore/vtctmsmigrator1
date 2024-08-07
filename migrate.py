from sqlalchemy import insert, text # type: ignore
from db import source_session, target_session
from mappings import TABLE_MAPPINGS

def migrate_table(source_table, target_table, mapping_function):
    source_data = source_session.execute(text(f"SELECT * FROM {source_table}")).fetchall()
    for row in source_data:
        mapped_data = mapping_function(row)
        target_session.execute(
            insert(target_table).values(**mapped_data)
        )
    target_session.commit()

def migrate():
    for source_table, target_info in TABLE_MAPPINGS.items():
        migrate_table(source_table, target_info['target_table'], target_info['mapping_function'])

if __name__ == "__main__":
    migrate()
