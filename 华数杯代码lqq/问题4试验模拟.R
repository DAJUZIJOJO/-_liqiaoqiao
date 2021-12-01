#数据预处理
n=read.csv("need_test1.csv",header=TRUE);n
n1<-n[,-1:-2]#去掉前两列
n1
n1$willing=as.factor(n1$willing)#将输入变量设置为
a=read.csv("data_buqi1.csv",header=TRUE);head(a)
test=as.data.frame(a);head(test)
a1<-a[,-1:-2]
a1$willing=as.factor(a1$willing)#将输入变量设置为
n1
k1=n1[2,];k1
k2=n1[7,];k2
k3=n1[14,];k3
a.pred=predict.boosting(Boot,newdata = k2)
a.pred$prob
k1_a1=data.frame()
i=1
#对客户1的满意度a1进行模拟##########
k1_a1=data.frame()
##
i=1
k1[1,1]=1.05*k1[1,1]
k1
k1[1,2]=1.05*k1[1,2]
k1[1,3]=1.05*k1[1,3]
k1[1,4]=0.95*k1[1,4]
k1[1,5]=1.05*k1[1,5]
k1[1,6]=0.95*k1[1,6]
k1[1,7]=0.95*k1[1,7]
k1[1,8]=1.05*k1[1,8]
k1
for ( i in range(1)) {
  j=8  #1-8
  a.pred=predict.boosting(Boot,newdata = k1)
  boos_test=a.pred$prob
  k1_a1[j,1]=boos_test[,2]
  k1_a1[j,2]=k1[1,8]
  i=i+1
}
k1_a1
write.csv(k1_a1,"k1.csv")


#对品牌2某客户
k2_a1=data.frame()
i=1
#对客户2的满意度进行模拟##########
k2_a1=data.frame()
##
i=1
#12837654
a.pred=predict.boosting(Boot,newdata = k2)
a.pred$prob
k2[1,1]=1.05*k2[1,1]
k2
k2[1,2]=1.05*k2[1,2]
k2[1,8]=1.05*k2[1,8]
k2[1,3]=1.05*k2[1,3]
k2[1,7]=1.05*k2[1,7]
k2[1,6]=k2[1,6]/0.95
k2[1,5]=1.05*k2[1,5]
k2[1,4]=1.05*k2[1,4]
k2
for ( i in range(1)) {
  j=8  #1-8
  a.pred=predict.boosting(Boot,newdata = k2)
  boos_test=a.pred$prob
  k2_a1[j,1]=boos_test[,2]
  k2_a1[j,2]=k2[1,8]
  i=i+1
}
k2_a1

write.csv(k2_a1,"k2.csv")
write.csv(k2,"k2.2.csv")

k3=n1[14,];k3
#对品牌3某客户
k3_a1=data.frame()
i=1
#对客户3的满意度进行模拟##########
k3_a1=data.frame()
##
i=1
#12837654
a.pred=predict.boosting(Boot,newdata = k3)
a.pred$prob
#15367248
k3[1,1]=1.05*k3[1,1];k3
k3[1,5]=k3[1,5]/1.05
k3[1,3]=1.05*k3[1,3]
k3[1,6]=k3[1,6]/1.05
k3[1,7]=k3[1,7]/1.05
k3[1,2]=1.05*k3[1,2]
k3[1,4]=k3[1,4]/1.05
k3[1,8]=1.05*k3[1,8]
k3
for ( i in range(1)) {
  j=8  #1-8
  a.pred=predict.boosting(Boot,newdata = k3)
  boos_test=a.pred$prob
  k3_a1[j,1]=boos_test[,2]
  k3_a1[j,2]=k3[1,8]
  i=i+1
}
k3_a1

write.csv(k3_a1,"k3.csv")

