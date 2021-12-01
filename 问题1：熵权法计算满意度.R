#满意度熵权法
a=read.csv("data_buqi1.csv",header=TRUE);head(a)
library(forecast)
library(XLConnect)
b=a[,3:10];head(b)

#normalize <- function(x) {return ((x - min(x)) / (max(x) - min(x)))}#0_1标准化
#data1=as.data.frame(lapply(b,normalize))#lapply是批量处理的高效函数
#head(data1)
data1=b
#求出所有样本对指标Xj的贡献总量
first1 <- function(data)
{
  x <- c(data)
  for(i in 1:length(data))
    x[i] = data[i]/sum(data[])
  return(x)
}
dataframe <- apply(data1,2,first1)
#将上步生成的矩阵每个元素变成每个元素与该ln（元素）的积并计算信息熵。
first2 <- function(data)
{
  x <- c(data)
  for(i in 1:length(data)){
    if(data[i] == 0){
      x[i] = 0
    }else{
      x[i] = data[i] * log(data[i])
    }
  }
  return(x)
}
dataframe1 <- apply(dataframe,2,first2)

k <- 1/log(length(dataframe1[,1]))
d <- -k * colSums(dataframe1);d <- 1-d #计算冗余度
w <- d/sum(d);w
write.csv(w,"熵值法权重1.csv")
sc=as.matrix(data1)%*%w;sc
write.csv(sc,"熵值法满意度1.csv")
#
a$sc=sc
head(sc)
