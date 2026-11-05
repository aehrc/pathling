// Databricks notebook source
// Check a, Scala half: installs the rule from Scala (not through py4j) and runs a few cases.
// COMMAND ----------
dbutils.widgets.text("base", "")
dbutils.widgets.text("out", "")
spike.DbxProbe.main(Array(dbutils.widgets.get("base"), dbutils.widgets.get("out"), "20000"))
