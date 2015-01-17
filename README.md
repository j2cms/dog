Parallel Clustering Algorithm for Categorical Data

兼容hadoop 1.xx和 Hadoop 2.xx 平台

目前已经实现的聚类算法：
1)kmodes
2)pclope

在Hadoop集群的master主机上配置如下：
将dog文件夹放入/usr/local/目录下,将dog_home.sh里的内容拷贝到/etc/profile文件里，并执行source /etc/profile使之生效.
dog中只需包含sbin和lib目录，lib目录中的dog.jar是由src源码编译而成(使用eclipse的export功能得到).


配置完成后，在系统任何目录都可以调用dog命令(注意：需要有要该目录创建文件的权限)

工具用法提示：: dog 或者 dog -h

算法的基本使用：

1)dog kmodes input output center k maxIter

分类数据聚类算法kmodes,参数解释如下:
  input         HDFS上的输入文件或者目录
  output        HDFS上的输出目录
  center        初始中心的文件
  k             簇个数
  maxIter       最大迭代次数  

2)dog pclope input output repulsion p maxIter isNumber

分类数据聚类算法pclope,参数解释如下:
  input         HDFS上的输入文件或者目录
  output        HDFS上的输出目录
  repulsion     排斥因子,符点型,一般大于1,值越大,生成聚类个数越多
  p             划分参数,整型,会生成p!个map,值越大,准确率越高
  maxIter       最大迭代次数
  isNumber      用1|0表示数据记录的属性项是否需要编号
