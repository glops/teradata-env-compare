# Teradata env compare

## Description

This tool compares two database environments of the same application and display a clean view of the differences:

- All kind of objects from DBC.tablesV
  - Table
  - View
  - Procedure
  - Macro
  - Join index
- Column from table and view
- Foreign keys
- Index
  - Index columns

It can compare two environments from two differents teradata instances.

## Getting started

### Install python modules

You can install all the required modules with:

```bash
pip install -r requirements.txt
```

### Database credentials

This tool require database access. You can configure your credentials in the file **config/database-conf.json**.

First, if it does not exist yet, copy the file **config/templates/database-conf.template.json** to **config/database-conf.json**.

Edit the copy and set the **defaultUser** and **defaultPassword** for at least one server. You can also define a **username** and **password** property specific to an environment.

The user must have SELECT access to the views in DBC.

You need also to define the **app** name and the pattern of the database names in **databaseNamePattern**.

For example, if your databases are named AAA_ODS_PRD, AAA_DWH_PRD... where AAA is the name of the application.

databaseNamePattern = `${app}_${db}_${env}`

And in the command line, you need to use ODS and DWH as database names.

### Run the script

```bash
python compare_env.py -e ENV1 -f ENV2 -d "DATABASE1 DATABASE2"
```

You can also filter the tables using `-t` option and ignore some object kinds or properties using the options `-i` and `-ip`. Check the help for more details :

```bash
python compare_env.py -h
```
