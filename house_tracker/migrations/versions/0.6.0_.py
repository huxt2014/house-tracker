"""empty message

Revision ID: 0.6.0
Revises: 0.5.0
Create Date: 2016-10-15 14:19:01.564286

"""

# revision identifiers, used by Alembic.
revision = '0.6.0'
down_revision = '0.5.0'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('community', 'last_track_week', new_column_name='last_batch_number',
                    existing_type=mysql.INTEGER)
    op.alter_column('community_record', 'create_week', new_column_name='batch_number',
                    existing_type=mysql.INTEGER, nullable=False)
    op.alter_column('house', 'last_track_week', new_column_name='last_batch_number',
                    existing_type=mysql.INTEGER)
    op.alter_column('house_record', 'create_week', new_column_name='batch_number',
                    existing_type=mysql.INTEGER, nullable=False)
    op.alter_column('job', 'target_url', new_column_name='target_uri',
                    existing_type=mysql.VARCHAR(collation=u'utf8_unicode_ci', length=1024))
    
    op.drop_column('community_record', 'house_parse_finish')
    op.drop_column('community_record', 'house_download_finish')
    
    op.add_column('job', sa.Column('house_id', mysql.INTEGER(), nullable=True))
    op.create_foreign_key(None, 'job', 'house', ['house_id'], ['id'])
    
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('job', sa.Column('target_url', mysql.VARCHAR(collation=u'utf8_unicode_ci', length=1024), nullable=True))
    op.drop_constraint(None, 'job', type_='foreignkey')
    op.drop_column('job', 'target_uri')
    op.drop_column('job', 'house_id')
    op.add_column('house_record', sa.Column('create_week', mysql.INTEGER(display_width=11), autoincrement=False, nullable=False))
    op.drop_column('house_record', 'batch_number')
    op.add_column('house', sa.Column('last_track_week', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.alter_column('house', 'new',
               existing_type=sa.BOOLEAN(),
               type_=mysql.TINYINT(display_width=1),
               existing_nullable=True)
    op.alter_column('house', 'available',
               existing_type=sa.BOOLEAN(),
               type_=mysql.TINYINT(display_width=1),
               existing_nullable=True)
    op.drop_column('house', 'last_batch_number')
    op.add_column('community_record', sa.Column('house_download_finish', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True))
    op.add_column('community_record', sa.Column('house_parse_finish', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True))
    op.add_column('community_record', sa.Column('create_week', mysql.INTEGER(display_width=11), autoincrement=False, nullable=False))
    op.drop_column('community_record', 'batch_number')
    op.add_column('community', sa.Column('last_track_week', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.alter_column('community', 'track_presale',
               existing_type=sa.BOOLEAN(),
               type_=mysql.TINYINT(display_width=1),
               existing_nullable=True)
    op.drop_column('community', 'last_batch_number')
    op.drop_column('community', 'company')
    ### end Alembic commands ###
