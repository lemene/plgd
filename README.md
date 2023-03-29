# 概述
plgd 是一个辅助编写集群作业脚本的工具，目前支持的系统有PBS、SGE、LSF和Slurm。


## Cluster
Cluster定义pipeline运行的集群系统，目前支持pbs、sge、lsf、slurm和local（单机运行）。Cluster抽象了下面重要的函数
1. new，创建类，如果检测到系统不支持该类型，则返回undef。
2. submit，提交作业
3. stop，停止作业
4. status，检查作业状态，检查作业是否在运行。

它有CLUSTER参数控制，如`CLUSTER = pbs:4`。分别设置集群类型和任务数目。
* 集群类型支持`pbs`、`sge`、`lsf`、`slurm`、`local`和`auto`。`auto`表示自动判断所处系统，
* 任务数目指pipeline同时提交的任务数目，支持0和正整数。0表示不限制任务数目。

# Examples
minimap2
racon
miniasm
## racon.pl
实现minimap2+racon的功能。主要的修改有部分：
1. minimap2部分，先将reads分成多个文件，然后并行比对到contigs上，最后合并比对结果。
2. racon部分，将contigs分成多个文件，然后并行polish，最后合并polish结果。


## miniasm.py
实现minimap2+miniasm+raocn的功能。