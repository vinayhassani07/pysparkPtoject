# Python code
#To reverse list

#input list
lst=[10, 11, 12, 13, 14, 15]
# the above input can also be given as
# lst=list(map(int,input().split()))
l=[] # empty list
# checking if elements present in the list or not
for i in lst:
    l.insert(0, i)
# printing result
print(l)
