from models.datagen import read_file, generate

export_job = read_file("input.json")

# print(export_job.to_json())

generate(export_job)
