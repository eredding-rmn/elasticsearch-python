# elasticsearch-python

this is where I'll stick my elasticsearch python tools.

## utilities/elastic_s3_snapshot.py
The idea was to be able to drop this on a cluster of boxes, and only have the current elected master carry out the operation; however, at this level, it appears there is something wonky with master detection.

eh, honestly, I'd just use [curator]https://github.com/elasticsearch/curator/ - snapshot support was added the day I completed this.  

There is an advantage: this auto-discovers your first repository and does some cluster discovery.  
