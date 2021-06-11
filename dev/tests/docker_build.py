from flowserv.controller.worker.docker import docker_build

image, logs = docker_build(name='test_build', requirements=['histore'])

print('\n'.join(logs))
print()
print(image)
