=========================
Post-Processing Workflows
=========================

Post-processing workflows are workflows in a template that run after a workflow run finished and the ranking of workflow evaluation results has changed. Post-processing workflows are defined sjust as the main workflow in the template. Post-processing workflows are executed by the same engine as the workflow.

The main difference is that post-porcessing workflows do not have access to any iser arguments. The only argument that can be references ``$[[runResultsFile]]`` is a JSON file that contains a list of all runs in the current ranking result together with the path to their result files. The format of the file is as follows:


.. code-block:: yaml

    postproc:
    	workflow:
    	    version: '0.3.0'
    	    inputs:
    	      files:
    		- 'postproc_code/'
    		- 'data/evaluate/labels.pkl'
    	    workflow:
    	      type: 'serial'
    	      specification:
    		steps:
    		  - environment: 'heikomueller/toptaggerdemo:1.0'
    		    commands:
    		- 'python postproc_code/plot-roc-auc.py \
    		      $[[runs]] data/evaluate/labels.pkl results'
    		- 'python postproc_code/plot-roc-bg-reject.py \
    		      $[[runs]] data/evaluate/labels.pkl results'
    	    outputs:
    	      files:
    	       - 'results/ROC-AUC.png'
    	       - 'results/ROC-BGR.png'
        inputs:
            files:
                - 'results/yProbBest.pkl'
            runs: '.runs'
    