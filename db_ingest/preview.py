"""Count/byte-bounded previews; callers continue processing omitted evidence."""
import json

class BoundedPreview:
    def __init__(self,max_bytes,limit=1000):
        self.max_bytes=max(0,int(max_bytes));self.limit=max(0,int(limit));self.items=[];self.total=0;self.used=2
    def append(self,item):
        self.total+=1
        if len(self.items)>=self.limit or self.max_bytes-self.used<128:return False
        size=sum(len(chunk) for chunk in json.JSONEncoder(ensure_ascii=True).iterencode(item))
        if self.used+size+2>self.max_bytes:return False
        self.items.append(item);self.used+=size+2;return True
    @property
    def truncated(self):return self.total>len(self.items)
    @property
    def full(self):return len(self.items)>=self.limit or self.max_bytes-self.used<128
