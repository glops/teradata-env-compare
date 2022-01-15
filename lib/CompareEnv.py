import re
from string import Template
from typing import Dict, List, Optional

import teradatasql
from colorama import init
from prompt_toolkit.shortcuts import ProgressBar
from termcolor import colored

from lib.DatabaseConfig import DatabaseConfig


class Environment(object):
    def __init__(self, name: str, dbConf: DatabaseConfig, number: int, color: str, dbList: List[str]):
        self.name = name
        self.dbCredentials = dbConf
        self.number = number
        self.color = color

        (server, env, username, password) = dbConf.getCredentials(name)
        self.code = env.code or env.name

        connectionStr = (
            '{"host":"' + server.host + '", "user":"' + username + '","password":"' + password + '", "cop": "false"}'
        )
        self.conn = teradatasql.connect(connectionStr)
        self.cur = self.conn.cursor()

        self.dbMap: Dict[str, str] = {}

        dbTemplate = Template(dbConf.conf.databaseNamePattern)

        for db in dbList:
            dbName = dbTemplate.substitute(app=dbConf.conf.app, db=db, env=self.code)
            self.dbMap[dbName] = db

        self.dbRegexStr = Template(dbConf.conf.databaseNamePattern).substitute(
            app=dbConf.conf.app, db="(.*)", env=self.code
        )


class CompareEnv(object):
    def __init__(self, dbCredentials: DatabaseConfig, ignoreList: List[str]):
        self.dbCredentials = dbCredentials
        self.app = self.dbCredentials.conf.app
        self.env1name: str = ""
        self.env2name: str = ""
        self.env1: Environment
        self.env2: Environment
        self.dbSuffixList: List[str] = []
        self.tableFilter: str = ""
        self.backupTableFilter = " and regexp_similar(TableName, '.*20[0-9]{6,12}','i') = 0"
        self.ignoreList = ignoreList
        self.ignoreProperties: List[str] = []
        self.envs: List[Environment] = []
        self.ddl = {}

        # fmt: off
        self.colors = (
            {"name": "cyan",    "attrs": []},
            {"name": "magenta", "attrs": []},
            {"name": "white",   "attrs": ["bold"]},
            {"name": "white",   "attrs": ["bold"]},
        )

        self.queries = [
            {
                "granularity": "table",
                "sql": self.getSqlDbcTables,
                "condition": True
            },
            {
                "granularity": "col",
                "sql": self.getSqlDbcColumns,
                "condition": True
            },
            {
                "granularity": "constraint",
                "sql": self.getSqlDbcConstraints,
                "condition": "constraints" not in ignoreList,
            },
            {
                "granularity": "constraintColumns",
                "sql": self.getSqlDbcConstraintColumns,
                "condition": "constraints" not in ignoreList,
            },
            {
                "granularity": "indices",
                "sql": self.getSqlDbcIndices,
                "condition": "indices" not in ignoreList,
            },
            {
                "granularity": "indexColumns",
                "sql": self.getSqlDbcIndexColumns,
                "condition": "indices" not in ignoreList,
            },
        ]
        # fmt: on

    def setTableFilter(self, tableFilter: Optional[str]):
        if tableFilter is not None and tableFilter.strip() != "":
            self.tableFilter = f" and TableName like '{tableFilter}'"

    def extractMetadata(self):
        maxRange = 2 * len(self.queries) + 2
        with ProgressBar(title=f"Extracting metadata") as pb:
            pb2 = pb(total=maxRange, remove_when_done=True)

            pb.title = f"Connecting to: {self.env1name}"
            self.env1 = Environment(self.env1name, self.dbCredentials, 1, "green", self.dbSuffixList)
            pb2.item_completed()
            pb.title = f"Connecting to: {self.env2name}"
            self.env2 = Environment(self.env2name, self.dbCredentials, 2, "yellow", self.dbSuffixList)
            pb2.item_completed()
            self.envs = [
                self.env1,
                self.env2,
            ]
            for env in self.envs:
                for query in self.queries:
                    granularity = query["granularity"]
                    pb.title = f"Extracting metadata {env.name}:{granularity}"
                    # print(f"\t{granularity}")
                    sql = query["sql"](env)
                    # print(sql)
                    res = env.cur.execute(sql)
                    objRows = self.array2Obj(res)
                    # print(objRows)
                    otherEnv = 1 if env.number == 2 else 2
                    self.fillArray(objRows, res, f"env{env.number}", f"env{otherEnv}", granularity)
                    pb2.item_completed()

            pb.title = "done"
            pb2.done = True

    def array2Obj(self, res: teradatasql.TeradataCursor):
        rows: List[List] = res.fetchall()
        header = res.description
        output = []
        if header is not None:
            for row in rows:
                newRow = {}
                for i, cell in enumerate(row):
                    if isinstance(cell, str):
                        newRow[header[i][0]] = cell.strip()
                    else:
                        newRow[header[i][0]] = cell
                output.append(newRow)

        return output

    def dbList2whereSqlList(self, env: Environment) -> str:
        dbListStr = ",".join(map(lambda db: f"'{db}'", env.dbMap.keys()))
        return f"({dbListStr})"

    def getSqlDbcTables(self, env: Environment):
        sql = f"""
            SELECT
                  REGEXP_REPLACE(DatabaseName, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as DatabaseName
                , TableName
                , TableKind
                , ProtectionType
                , JournalFlag
                , CommentString
                , PIColumnCount
                , PartitioningLevels
            FROM DBC.TablesV
            where DataBaseName in {self.dbList2whereSqlList(env)}
                {self.tableFilter}
                {self.backupTableFilter}
            order by DatabaseName, TableName
        """
        return sql

    def getSqlDbcColumns(self, env: Environment):
        return f"""
            SELECT
                  REGEXP_REPLACE(DatabaseName, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as DatabaseName
                , TableName
                , ColumnName
                , ColumnFormat
                , CharType
                , Nullable
                , DefaultValue
                --, CommentString
                , UpperCaseFlag
            FROM DBC.ColumnsV
            where DatabaseName in {self.dbList2whereSqlList(env)}
                {self.tableFilter}
                {self.backupTableFilter}
            order by DatabaseName, TableName, ColumnName
            """

    def getSqlDbcConstraints(self, env: Environment):
        return f"""
            SELECT
                  REGEXP_REPLACE(ParentDB, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as DatabaseName
                , ParentTable                                       as TableName
                , coalesce(IndexName, ChildDatabase || '_' || ChildTable || '_' || trim(IndexID))   as ConstraintName
                , REGEXP_REPLACE(ChildDB, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as ChildDatabase
                , IndexName
                , ChildTable
            FROM DBC.All_RI_ChildrenV
            where ParentDB in {self.dbList2whereSqlList(env)}
                {self.tableFilter}
                {self.backupTableFilter}
            group by 1,2,3,4,5,6
            order by DatabaseName, TableName, ConstraintName
        """

    def getSqlDbcConstraintColumns(self, env: Environment):
        return f"""
            SELECT
                  REGEXP_REPLACE(ParentDB, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as DatabaseName
                , ParentTable                                        as TableName
                , coalesce(IndexName, REGEXP_REPLACE(ChildDB, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i')
                    || '_' || ChildTable || '_' || trim(IndexID))   as ConstraintName
                , ParentKeyColumn                                    as ColumnName
                , ChildKeyColumn
            FROM DBC.All_RI_ChildrenV
            where ParentDB in {self.dbList2whereSqlList(env)}
                {self.tableFilter}
                {self.backupTableFilter}
            order by DatabaseName, TableName, ConstraintName, ChildKeyColumn
        """

    def getSqlDbcIndices(self, env: Environment):
        return f"""
            SELECT
                  REGEXP_REPLACE(DatabaseName, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as DatabaseName
                , TableName
                --, case when IndexName is not null and IndexType <> 'P' then IndexName else TableName || '_' || IndexType || '_' || trim(IndexNumber) end as IndexCode
                , TableName || '_' || IndexType || '_' || trim(IndexNumber) as IndexCode
                , IndexName
                , IndexNumber
                , IndexType
                , UniqueFlag
            FROM DBC.IndicesV i
            where DatabaseName in {self.dbList2whereSqlList(env)}
                {self.tableFilter}
                {self.backupTableFilter}
            group by 1,2,3,4,5,6,7
            order by 1,2,3,4,5,6,7
        """

    def getSqlDbcIndexColumns(self, env: Environment):
        return f"""
            SELECT
                  REGEXP_REPLACE(DatabaseName, '^{env.dbRegexStr}$', '\\1', 1, 1, 'i') as DatabaseName
                , TableName
                --, case when IndexName is not null and IndexType <> 'P' then IndexName else TableName || '_' || IndexType || '_' || trim(IndexNumber) end as IndexCode
                , TableName || '_' || IndexType || '_' || trim(IndexNumber) as IndexCode
                , ColumnName
                , ColumnPosition
            FROM DBC.IndicesV
            where DatabaseName in {self.dbList2whereSqlList(env)}
                {self.tableFilter}
                {self.backupTableFilter}
            order by 1,2,3,4,5
        """

    def fillArray(self, dbcColumns, res, envName, otherEnv, granularity):
        for line in dbcColumns:
            dbName = line["DatabaseName"].upper()
            tbName = line["TableName"].upper()

            if (
                re.search("_P[0-9]{3}_S[0-9]{3}$", tbName)
                or re.search("_[0-9]{8}$", tbName)
                or tbName.endswith(("_1", "_2", "_ET"))
            ):
                continue

            if dbName not in self.ddl:
                self.ddl[dbName] = {"name": dbName, envName: True, otherEnv: False, "tables": {}}
            else:
                self.ddl[dbName][envName] = True

            tables = self.ddl[dbName]["tables"]
            if tbName not in tables:
                tables[tbName] = {
                    "name": tbName,
                    envName: True,
                    otherEnv: False,
                    "columns": {},
                    "constraints": {},
                    "indices": {},
                }
            else:
                tables[tbName][envName] = True

            if granularity == "table":
                tables[tbName][envName + "Properties"] = self.fillColProperties(line, res)

            if granularity == "col":
                colName = line["ColumnName"].upper()
                columns = tables[tbName]["columns"]
                if colName not in columns:
                    columns[colName] = {"name": colName, envName: True, otherEnv: False}
                else:
                    columns[colName][envName] = True

                columns[colName][envName + "Properties"] = self.fillColProperties(line, res)

            if granularity == "constraint":
                constraintName = line["ConstraintName"].upper()
                constraints = tables[tbName]["constraints"]
                if constraintName not in constraints:
                    constraints[constraintName] = {
                        "name": constraintName,
                        "columns": {},
                        envName: True,
                        otherEnv: False,
                    }
                else:
                    constraints[constraintName][envName] = True

                constraints[constraintName][envName + "Properties"] = self.fillColProperties(line, res)

            if granularity == "constraintColumns":
                constraintName = line["ConstraintName"].upper()
                constraints = tables[tbName]["constraints"]
                colName = line["ColumnName"].upper()
                columns = constraints[constraintName]["columns"]
                if colName not in columns:
                    columns[colName] = {"name": colName, envName: True, otherEnv: False}
                else:
                    columns[colName][envName] = True

                columns[colName][envName + "Properties"] = self.fillColProperties(line, res)

            if granularity == "indices":
                indexCode = line["IndexCode"].upper()
                indices = tables[tbName]["indices"]
                if indexCode not in indices:
                    indices[indexCode] = {"name": indexCode, "columns": {}, envName: True, otherEnv: False}
                else:
                    indices[indexCode][envName] = True

                indices[indexCode][envName + "Properties"] = self.fillColProperties(line, res)

            if granularity == "indexColumns":
                indexCode = line["IndexCode"].upper()
                indices = tables[tbName]["indices"]
                colName = line["ColumnName"].upper()
                columns = indices[indexCode]["columns"]
                if colName not in columns:
                    columns[colName] = {"name": colName, envName: True, otherEnv: False}
                else:
                    columns[colName][envName] = True

                columns[colName][envName + "Properties"] = self.fillColProperties(line, res)

    def fillColProperties(self, line, res):
        properties = {}
        for colDesc in res.description:
            colName = colDesc[0]
            if colName not in ["DatabaseName", "TableName", "ColumnName", "IndexCode", "ConstraintName"]:
                if type(line[colName]) is str:
                    properties[colName] = line[colName].replace("\r", "\n")
                else:
                    properties[colName] = line[colName]

        return properties

    def delete_keys_from_dict(self, dict_del, lst_keys):
        for k in lst_keys:
            try:
                del dict_del[k]
            except KeyError:
                pass
        for v in dict_del.values():
            if isinstance(v, dict):
                self.delete_keys_from_dict(v, lst_keys)

        return dict_del

    def merge(self, a, b, path=None):
        "merges b into a"
        if path is None:
            path = []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self.merge(a[key], b[key], path + [str(key)])
                else:
                    a[key] = b[key]
            else:
                a[key] = b[key]
        return a

    def mergeObjects(self, obj1, obj2):

        if obj2["env1"]:
            newObj2 = self.delete_keys_from_dict(obj2, ["env2"])
        else:
            newObj2 = self.delete_keys_from_dict(obj2, ["env1"])

        return self.merge(obj1, newObj2)

    def matchUnnamedObjects(self, objList, objType):
        newObjectList = {}

        for name, obj in objList.items():
            if not (obj["env1"] and obj["env2"]):
                trueId = ""
                for name, col in obj["columns"].items():

                    if objType == "indices":
                        trueId += "#" + name
                    elif objType == "constraints":
                        if obj["env1"]:
                            trueId += "#" + col["env1Properties"]["ChildKeyColumn"]
                        else:
                            trueId += "#" + col["env2Properties"]["ChildKeyColumn"]

                if objType == "constraints":
                    if obj["env1"]:
                        childTable = obj["env1Properties"]["ChildDatabase"] + "." + obj["env1Properties"]["ChildTable"]
                    else:
                        childTable = obj["env2Properties"]["ChildDatabase"] + "." + obj["env2Properties"]["ChildTable"]

                    trueId = childTable + trueId
                obj["trueId"] = trueId
                if trueId in newObjectList:

                    newObjectList[trueId] = self.mergeObjects(obj, newObjectList[trueId])
                    # pp.pprint(newObjectList[trueId])
                else:
                    newObjectList[trueId] = obj
            else:
                newObjectList[name] = obj

        return newObjectList

    # Add one tab to the beginning of each line
    def addTab(self, string):
        return "\t".join(string.splitlines(True))

    def getDiffObj(self, objects, lvl):
        diffObjs = ""
        for name, obj in sorted(objects.items()):
            # if obj['name'] == 'REF_TERRITORY' or obj['name'] == 'REF_TERRITORY':
            #     pp.pprint(obj)

            diffObj = ""
            if not obj["env1"]:
                diffObj = " not in " + colored(self.env1.name, self.env1.color)
            elif not obj["env2"]:
                diffObj = " not in " + colored(self.env2.name, self.env2.color)
            else:
                diffObjProperties = self.getDiffProperties(obj)
                if diffObjProperties != "":
                    diffObj = diffObjProperties
                for k in obj.keys():
                    if type(obj[k]) is dict and k != "env1Properties" and k != "env2Properties":
                        if k == "constraints" or k == "indices":
                            obj[k] = self.matchUnnamedObjects(obj[k], k)
                        diffSubObjs = self.getDiffObj(obj[k], lvl + 1)
                        if diffSubObjs != "":
                            diffObj += self.addTab("\n" + k + diffSubObjs)

            tableKind = ""

            if "env1Properties" in obj:
                if "TableKind" in obj["env1Properties"]:
                    tableKind = obj["env1Properties"]["TableKind"]
            if tableKind == "":
                if "env2Properties" in obj:
                    if "TableKind" in obj["env2Properties"]:
                        tableKind = obj["env2Properties"]["TableKind"]
            if tableKind != "":
                objName = "(" + tableKind + ") " + obj["name"]
            else:
                objName = obj["name"]

            if diffObj != "":
                diffObjs += (
                    "\n" + colored(objName, self.colors[lvl]["name"], attrs=self.colors[lvl]["attrs"]) + diffObj
                )

        if diffObjs != "" and lvl > 0:
            return self.addTab(diffObjs)
        elif diffObjs != "" and lvl == 0:
            return diffObjs
        else:
            return ""

    def getDiffProperties(self, obj):
        if "env1Properties" not in obj:
            return ""
        diffObjProperties = ""
        # pp.pprint(obj)
        for propName, val1 in obj["env1Properties"].items():
            if propName.lower() == "commentstring" and "comments" in self.ignoreProperties:
                continue
            if propName.lower() in self.ignoreProperties:
                continue

            if val1 is None:
                val1 = "Null"

            val2 = obj["env2Properties"][propName]
            if val2 is None:
                val2 = "Null"

            if propName == "ColumnFormat":
                if val1.upper() == "YYYY-MM-DD":
                    val1 = "YY/MM/DD"
                if val2.upper() == "YYYY-MM-DD":
                    val2 = "YY/MM/DD"

            if val1 != val2:
                diffObjProperties += (
                    "\n" + propName + " : " + colored(val1, self.env1.color) + " -> " + colored(val2, self.env2.color)
                )

        if diffObjProperties != "":
            return self.addTab(diffObjProperties)
        else:
            return ""
