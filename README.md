# SQL Broadcaster (sbroad)

Current library contains a query planner and executor for the distributed SQL in Tarantool `vshard` cluster.

## Getting started

First, you need an already installed `rust`, `tarantool` and `cartridge-cli` in your environment. Then run
```bash
git clone https://gitlab.com/picodata/picodata/sbroad.git
cd sbroad
make test_all
```
An example of the `sbroad` integration with Tarantool can be found in the `test_app` folder.

## Architecture

`sbroad` library consists of the three main parts:

- SQL frontend
- planner
- executor

We try to keep the planner independent from the other modules. For example, we can implement some other frontend and use already implemented planner and executor.

More information about the ideas behind `sbroad` can be found in the [design presentation](doc/design/sbroad.pdf). Internal details can be found with the module documentation:
```
cargo doc --open
```

### SQL frontend

We use a custom [PEG](src/frontend/sql/grammar.pest) for the `pest` parser generator to compile a SQL query and a `vshard` cluster schema into the planner's intermediate representation (IR).
The SQL query passes three main steps:

- the parse tree (PT) iterator. It is produced by `pest` parser generated for our grammar.
- abstract syntax tree (AST). It is a wrapper over the PT nodes to transform them to a more IR friendly tree. Also we have our own iterator to traverse the AST in the convenient order.
- plan intermediate representation (IR). A fully self-describing tree with the information from the SQL query and a table schema. It can be used by the planner for further transformations.

### Planner

The main goal of the planner is to insert the data motion nodes in the IR tree to instruct the executor where and what portions of data to transfer between Tarantool instancies in the cluster.

To make these motions efficient (i.e. transfer as less data amount as possible) we rely on the tuple distribution information. When we detect a distribution conflict, a motion node should be inserted to solve it. So, the better information about distribution we have in every tuple of the IR tree, the less data we move over the cluster. To achieve it, the planner applies many IR transformations other than the motion derivation.

### Executor
An executor is located on the coordinator node in the `vshard` cluster. It collects all the intermediate results of the plan execution in memory and executes IR plan tree in the bottom-up manner.

1. Collects all the motion nodes from the bottom layer. In theory all the motions in the same layer can be executed in parallel (we haven't implemented it yet).
1. For every motion
   - inspect the IR sub-tree and detect the buckets to execute the query.
   - build a valid SQL query from IR sub-tree.
   - map-reduce this SQL query (we send it to the shards deduced from the buckets).
   - build a virtual table with the query results, corresponding to the original motion.
1. Move to the next motion layer in the IR tree.
1. For every motion
   - link the virtual table results of the motion from the previous layer we depend on.
   - inspect the IR sub-tree and detect the buckets to execute the query.
   - build a valid SQL query from IR sub-tree (virtual table is serialized as `VALUES (), .., ()`).
   - map-reduce this SQL query.
   - build a virtual table with the query results, corresponding to the original motion.
1. Goto step `3` till we are done with motion layers.
1. Execute the final IR top subtree and return the user a final result.

The most complicated logic here can be found in the IR to SQL serialization (SQL backend) and bucket deduction (to execute the result SQL query on).
