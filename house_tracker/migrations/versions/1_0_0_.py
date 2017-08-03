"""empty message

Revision ID: 7414bb0b5037
Revises: 
Create Date: 2017-08-03 10:39:13.732635

"""

# revision identifiers, used by Alembic.
revision = '1.0.0'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

import house_tracker.models.base

def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('batch_job',
    sa.Column('status', mysql.VARCHAR(length=16), nullable=True),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('batch_number', mysql.INTEGER(), nullable=False),
    sa.Column('type', sa.BINARY(length=8), nullable=False),
    sa.PrimaryKeyConstraint('batch_number', 'type'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('district',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('name', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('outer_id', mysql.INTEGER(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('area',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('name', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('district_id', mysql.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['district_id'], ['district.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('job',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('status', mysql.VARCHAR(length=16), nullable=True),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('batch_number', mysql.INTEGER(), nullable=False),
    sa.Column('batch_type', sa.BINARY(length=8), nullable=False),
    sa.Column('parameters', house_tracker.models.base.PickleType(), nullable=True),
    sa.Column('type', mysql.VARCHAR(length=16), nullable=True),
    sa.Column('community_id', mysql.INTEGER(), nullable=True),
    sa.Column('district_id', mysql.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['batch_number', 'batch_type'], ['batch_job.batch_number', 'batch_job.type'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('community',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('district_id', mysql.INTEGER(), nullable=True),
    sa.Column('area_id', mysql.INTEGER(), nullable=True),
    sa.Column('outer_id', mysql.VARCHAR(length=128), nullable=True),
    sa.Column('name', mysql.VARCHAR(length=64), nullable=False),
    sa.Column('type', mysql.VARCHAR(length=16), nullable=False),
    sa.Column('average_price', mysql.INTEGER(), nullable=True),
    sa.Column('house_available', mysql.INTEGER(), nullable=True),
    sa.Column('sold_last_season', mysql.INTEGER(), nullable=True),
    sa.Column('view_last_month', mysql.INTEGER(), nullable=True),
    sa.Column('total_number', mysql.INTEGER(), nullable=True),
    sa.Column('total_area', mysql.FLOAT(), nullable=True),
    sa.Column('location', mysql.VARCHAR(length=1024), nullable=True),
    sa.Column('company', mysql.VARCHAR(length=1024), nullable=True),
    sa.Column('track_presale', sa.BOOLEAN(), nullable=True),
    sa.Column('presale_url_name', mysql.VARCHAR(length=1024), nullable=True),
    sa.ForeignKeyConstraint(['area_id'], ['area.id'], ),
    sa.ForeignKeyConstraint(['district_id'], ['district.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('land_sold_record',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('district_id', mysql.INTEGER(), nullable=True),
    sa.Column('area_id', mysql.INTEGER(), nullable=True),
    sa.Column('land_name', mysql.VARCHAR(length=256), nullable=True),
    sa.Column('description', mysql.TEXT(), nullable=True),
    sa.Column('boundary', mysql.TEXT(), nullable=True),
    sa.Column('record_no', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('sold_date', sa.DATE(), nullable=True),
    sa.Column('land_price', mysql.INTEGER(), nullable=True),
    sa.Column('company', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('land_area', mysql.INTEGER(), nullable=True),
    sa.Column('plot_ratio', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('status', mysql.VARCHAR(length=64), nullable=True),
    sa.ForeignKeyConstraint(['area_id'], ['area.id'], ),
    sa.ForeignKeyConstraint(['district_id'], ['district.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('community_record',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('community_id', mysql.INTEGER(), nullable=False),
    sa.Column('batch_number', mysql.INTEGER(), nullable=False),
    sa.Column('batch_type', sa.BINARY(length=8), nullable=False),
    sa.Column('average_price', mysql.INTEGER(), nullable=True),
    sa.Column('house_available', mysql.INTEGER(), nullable=True),
    sa.Column('sold_last_season', mysql.INTEGER(), nullable=True),
    sa.Column('view_last_month', mysql.INTEGER(), nullable=True),
    sa.Column('new_number', mysql.INTEGER(), nullable=True),
    sa.Column('missing_number', mysql.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['batch_number', 'batch_type'], ['batch_job.batch_number', 'batch_job.type'], ),
    sa.ForeignKeyConstraint(['community_id'], ['community.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('house',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('community_id', mysql.INTEGER(), nullable=False),
    sa.Column('outer_id', mysql.VARCHAR(length=128), nullable=False),
    sa.Column('area', mysql.FLOAT(), nullable=True),
    sa.Column('room', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('build_year', mysql.INTEGER(), nullable=True),
    sa.Column('floor', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('price_origin', mysql.INTEGER(), nullable=True),
    sa.Column('last_batch_number', mysql.INTEGER(), nullable=True),
    sa.Column('new', sa.BOOLEAN(), nullable=True),
    sa.Column('available', sa.BOOLEAN(), nullable=True),
    sa.Column('available_change_times', mysql.INTEGER(), nullable=True),
    sa.Column('price', mysql.INTEGER(), nullable=True),
    sa.Column('view_last_month', mysql.INTEGER(), nullable=True),
    sa.Column('view_last_week', mysql.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['community_id'], ['community.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('land',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('sold_record_id', mysql.INTEGER(), nullable=True),
    sa.Column('district_id', mysql.INTEGER(), nullable=True),
    sa.Column('area_id', mysql.INTEGER(), nullable=True),
    sa.Column('description', mysql.VARCHAR(length=64), nullable=True),
    sa.Column('plot_ratio', mysql.FLOAT(), nullable=True),
    sa.Column('type', mysql.VARCHAR(length=64), nullable=False),
    sa.ForeignKeyConstraint(['area_id'], ['area.id'], ),
    sa.ForeignKeyConstraint(['district_id'], ['district.id'], ),
    sa.ForeignKeyConstraint(['sold_record_id'], ['land_sold_record.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('presale_permit',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('community_id', mysql.INTEGER(), nullable=False),
    sa.Column('serial_number', mysql.VARCHAR(length=32), nullable=True),
    sa.Column('description', mysql.VARCHAR(length=1024), nullable=True),
    sa.Column('sale_date', sa.DATE(), nullable=True),
    sa.Column('total_number', mysql.INTEGER(), nullable=True),
    sa.Column('normal_number', mysql.INTEGER(), nullable=True),
    sa.Column('total_area', mysql.FLOAT(), nullable=True),
    sa.Column('normal_area', mysql.FLOAT(), nullable=True),
    sa.Column('status', mysql.VARCHAR(length=64), nullable=True),
    sa.ForeignKeyConstraint(['community_id'], ['community.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    op.create_table('house_record',
    sa.Column('id', mysql.INTEGER(), nullable=False),
    sa.Column('created_at', mysql.DATETIME(), nullable=True),
    sa.Column('last_modified_at', mysql.DATETIME(), nullable=True),
    sa.Column('community_id', mysql.INTEGER(), nullable=False),
    sa.Column('house_id', mysql.INTEGER(), nullable=False),
    sa.Column('batch_number', mysql.INTEGER(), nullable=False),
    sa.Column('batch_type', sa.BINARY(length=8), nullable=False),
    sa.Column('price', mysql.INTEGER(), nullable=True),
    sa.Column('price_change', mysql.INTEGER(), nullable=True),
    sa.Column('view_last_month', mysql.INTEGER(), nullable=True),
    sa.Column('view_last_week', mysql.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['batch_number', 'batch_type'], ['batch_job.batch_number', 'batch_job.type'], ),
    sa.ForeignKeyConstraint(['community_id'], ['community.id'], ),
    sa.ForeignKeyConstraint(['house_id'], ['house.id'], ),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_collate='utf8_bin',
    mysql_engine='InnoDB'
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('house_record')
    op.drop_table('presale_permit')
    op.drop_table('land')
    op.drop_table('house')
    op.drop_table('community_record')
    op.drop_table('land_sold_record')
    op.drop_table('community')
    op.drop_table('job')
    op.drop_table('area')
    op.drop_table('district')
    op.drop_table('batch_job')
    # ### end Alembic commands ###
