import argparse

from colorama import init

from lib.CompareEnv import CompareEnv
from lib.DatabaseConfig import DatabaseConfig

if __name__ == "__main__":
    init()

    dbCredentials = DatabaseConfig()

    envlist = dbCredentials.getEnvironmentList()

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env1", choices=envlist, help="Environnement", required=True)
    parser.add_argument("-f", "--env2", choices=envlist, help="Environnement", required=True)
    parser.add_argument("-d", "--databases", help="List of databases to compare separated by comma", required=True)
    parser.add_argument("-t", "--tablefilter", help="Filter on tablename. Use % sign for partial search")
    parser.add_argument(
        "-i",
        "--ignore-objects",
        help="List of objects to ignore separated by comma in (constraints,indices)",
    )
    parser.add_argument(
        "-ip",
        "--ignore-properties",
        help="List of properties to ignore separated by comma in (comments,indexname,ProtectionType)",
    )
    args = parser.parse_args()

    compareEnv = CompareEnv(
        dbCredentials=dbCredentials,
        ignoreList=list(map(str.lower, (args.ignore_objects or "").split(","))),
    )

    compareEnv.env1name = args.env1
    compareEnv.env2name = args.env2

    compareEnv.ignoreProperties = list(map(str.lower, (args.ignore_properties or "").split(",")))

    compareEnv.dbSuffixList = args.databases.split(",")
    compareEnv.setTableFilter(args.tablefilter)

    compareEnv.extractMetadata()

    print(compareEnv.getDiffObj(compareEnv.ddl, 0))
