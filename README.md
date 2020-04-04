# 概述
plgd 是一个辅助编写集群作业脚本的工具，目前支持的系统有PBS、SGE、LSF和Slurm。


# 例子
## racon.pl
实现minimap2+racon的功能。主要的修改有部分：
1. minimap2部分，先将reads分成多个文件，然后并行比对到contigs上，最后合并比对结果。
2. racon部分，将contigs分成多个文件，然后并行polish，最后合并polish结果。


## miniasm.py
实现minimap2+miniasm+raocn的功能。