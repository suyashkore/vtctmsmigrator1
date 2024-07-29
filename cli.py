import click
from migrate import migrate_table
from mappings import TABLE_MAPPINGS

@click.group()
def cli():
    pass

@cli.command()
@click.option('--table', prompt='Table to migrate', help='The name of the source table to migrate.')
def migrate(table):
    if table in TABLE_MAPPINGS:
        target_info = TABLE_MAPPINGS[table]
        migrate_table(table, target_info['target_table'], target_info['mapping_function'])
        click.echo(f'Successfully migrated {table} to {target_info["target_table"]}.')
    else:
        click.echo('Table not found in mappings.')

if __name__ == "__main__":
    cli()
