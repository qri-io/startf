
def transform(ds, ctx):
  ds.set_body([1, 1.5, False, 'a','b','c', { "a" : 1, "b" : True }, [1,2]])
  return ds