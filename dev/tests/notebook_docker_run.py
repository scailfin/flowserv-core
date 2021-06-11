
import sys

from flowserv.controller.worker.docker import NotebookDockerWorker
from flowserv.model.workflow.step import NotebookStep
from flowserv.volume.fs import FileSystemStorage

fs = FileSystemStorage(basedir=sys.argv[1])

worker = NotebookDockerWorker(identifier='W0')

step = NotebookStep(
    identifier='say_hello_papermill',
    notebook='notebooks/HelloWorld.ipynb',
    params=['inputfile', 'outputfile', 'greeting']
)

args = {'inputfile': 'data/names.txt', 'outputfile': 'results/greetings.txt', 'greeting': 'Hey'}

result = worker.exec(step=step, context=args, store=fs)
print('\n'.join(result.stdout))
print('\n'.join(result.stderr))
