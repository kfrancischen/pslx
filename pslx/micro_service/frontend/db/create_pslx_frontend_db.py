from migrate.versioning import api
from pslx.micro_service.frontend import pslx_frontend_db, pslx_frontend_ui_app
from pslx.util.file_util import FileUtil


def main():
    pslx_frontend_db.create_all()
    sqlalchemy_migration_repo = pslx_frontend_ui_app.config['SQLALCHEMY_MIGRATE_REPO']
    sqlalchemy_database_uri = pslx_frontend_ui_app.config['SQLALCHEMY_DATABASE_URI']
    if not FileUtil.does_dir_exist(sqlalchemy_migration_repo):
        api.create(sqlalchemy_database_uri, 'database repository')
        api.version_control(sqlalchemy_database_uri, sqlalchemy_migration_repo)
    else:
        api.version_control(sqlalchemy_database_uri, sqlalchemy_migration_repo, api.version(sqlalchemy_migration_repo))


if __name__ == '__main__':
    main()
