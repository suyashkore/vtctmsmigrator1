# cli.py

import click # type: ignore
from migrate import migrate_table
from mappings import TABLE_MAPPINGS

@click.group()
def cli():
    pass

@cli.command()
@click.option('--table', prompt='Table to migrate (or "all" to migrate all tables)', help='The name of the source table to migrate.')
def migrate(table):
    if table == 'all':
        for table, target_info in TABLE_MAPPINGS.items():
            migrate_table(table, target_info['target_table'], target_info['mapping_function'])
            click.echo(f'Successfully migrated {table} to {target_info["target_table"]}.')
    elif table in TABLE_MAPPINGS:
        target_info = TABLE_MAPPINGS[table]
        migrate_table(table, target_info['target_table'], target_info['mapping_function'])
        click.echo(f'Successfully migrated {table} to {target_info["target_table"]}.')
    else:
        click.echo('Table not found in mappings.')

if __name__ == "__main__":
    cli()
