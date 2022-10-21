import time

from models.datagen import read_file, generate

st = time.time()
export_job = read_file("input.json")
generate(export_job)
print("Time taken = %f" % (time.time() - st))
