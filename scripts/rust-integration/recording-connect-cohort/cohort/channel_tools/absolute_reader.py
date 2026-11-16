import os
def descriptor_absent(fd,inode):
 if type(fd)is not int or fd<0 or type(inode)is not int:return False
 try:return os.fstat(fd).st_ino!=inode
 except OSError:return True
