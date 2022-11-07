# Databricks notebook source
from pyspark.sql import functions as psf
from pyspark.sql import types as t
import torch
import mlflow
import MainModel
import pandas as pd

_MODEL_NAME = "sewer-cctv-image-classifier-14"
_MODEL_WEIGTHS_LOCATION = '/dbfs/mnt/blob-sewercctvmodel/mxnet_resnet152_to_pth_sewer_image_classifier.pth'

torch_model = torch.load(f=_MODEL_WEIGTHS_LOCATION)

# COMMAND ----------

data = [
    ["0_ryy1flvs", 0, "/dbfs/mnt/blob-sewercctvmodel/test_image_0_ryy1flvs-0.png"]
]

df = (spark.createDataFrame(data, ["video_id", "timestamp", "path"]))
display(df)

# COMMAND ----------

from torchvision.datasets.folder import default_loader  # private API

class ImageDataset(torch.utils.data.Dataset):
        def __init__(self, paths, transform=None):
            self.paths = paths
            self.transform = transform
        def __len__(self):
            return len(self.paths)
        def __getitem__(self, index):
            image = default_loader(self.paths.loc[index])
            if self.transform is not None:
                image = self.transform(image)
            return image

# COMMAND ----------

class PytorchWrapper(mlflow.pyfunc.PythonModel):
     # Artifacts global uri/location
    artifacts_global : dict = {
        "RESNET_MODEL_PATH" : _MODEL_WEIGTHS_LOCATION
    }
       
    def load_context(self, context):
        '''
        this section of code is tasked with setting up the model.
        '''
        
        import os
        from torchvision import transforms

        # need to set up GPU
        self.device = 'cpu'
        
          # define transformation pipeline
        self.transform = transforms.Compose([transforms.Resize(224),
                                             transforms.CenterCrop(224),
                                             transforms.PILToTensor()
                                            ])
        if context:
            # At Runtime
            resnet_path = os.path.abspath(context.artifacts["RESNET_MODEL_PATH"])
        else:
            # During training/interactive 
            resnet_path = self.artifacts_global['RESNET_MODEL_PATH']

        print(f'start loading resnet model')
        self.resnet_model = torch.load(f=resnet_path)
        self.resnet_model = self.resnet_model.eval().to(self.device) # send the model to device
        print(f'end loading resnet model')
        
        
    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        import torch
        import pandas as pd
            
        classes = [['Mass Roots >=75%', 100], 
                   ['Mass Roots 51-75%', 70], 
                   ['Fine Roots 21-50%', 30], 
                   ['Fine Roots <=20%', 5], 
                   ['Junction Mass Roots', 30], 
                   ['Tap Roots', 50], 
                   ['Break', 30], 
                   ['Break Pieces Missing', 50], 
                   ['Crack Longitudinal', 5], 
                   ['Crack Circumferential', 5], 
                   ['Crack Complex', 10], 
                   ['Infiltration', 5], 
                   ['Joint Displaced Radially', 2], 
                   ['No Defect', 0]
                  ]
            
        images = ImageDataset(model_input['path'], transform=self.transform)
        loader = torch.utils.data.DataLoader(images, batch_size=8, num_workers=2)
        
        all_predictions = []
        
        with torch.no_grad():
            for batch in loader:
                predictions = list(self.resnet_model(batch.to(self.device).float()).cpu())
                for prediction in predictions:
                    all_predictions.append(prediction)
                
                    
        return pd.DataFrame({'video_id': model_input['video_id'],
                             'timestamp': model_input['timestamp'],
                             'path': model_input['path'],
                             'defect': map(lambda x: classes[torch.argmax(x).item()][0], all_predictions), 
                             'confidence': map(lambda x: torch.max(x).item(), all_predictions), 
                             'score': map(lambda x: classes[torch.argmax(x).item()][1], all_predictions)
                            })

# COMMAND ----------

wrappedModel = PytorchWrapper()
wrappedModel.load_context(context=None)
prediction = wrappedModel.predict(context=None, model_input=df.toPandas())
prediction

# COMMAND ----------

signature = mlflow.models.signature.infer_signature(df.toPandas(), prediction) 

# COMMAND ----------

conda_environment = {
    "name": "mlflow-env",
    "channels": ["conda-forge"],
    "dependencies": [
        "python=3.8.10",
        "pip<=21.0.1",
        
        {
            "pip": [
                "mlflow==1.27.0",
                "astunparse==1.6.3",
                "cloudpickle==1.6.0",
                "dill==0.3.2",
                "ipython==7.22.0",
                "torch",
                "torchvision",
                "/dbfs/mnt/blob-sewercctvmodel/MainModel-0.1.0-py3-none-any.whl"
            ],
        },
    ],
}


# COMMAND ----------

artifacts_global = {
    "RESNET_MODEL_PATH" : _MODEL_WEIGTHS_LOCATION
}

# COMMAND ----------

_MODEL_NAME = "sewer-cctv-image-classifier"
_MODEL_DESCRIPTION = 'Image classifier to identify 14 different defects within sewer cctv footage. \n\
                                  The image classifier is implemented using the pytorch deep learning framework and the resnet-152 deep learning convolutional neural network.\n\
                                  The 14 defects include:\n\
                                      1. Mass Roots >75%\n\
                                      2. Mass Roots 50-75%\n\
                                      3. Mass Roots 21-50%\n\
                                      4. Fine Roots <20%\n\
                                      5. Tap Roots\n\
                                      6. Junction Mass Roots\n\
                                      7. Crack Longitudinal\n\
                                      8. Crack Circumferential\n\
                                      9. Crack Complex\n\
                                      10. Breaks\n\
                                      11. Breaks Pieces Missing\n\
                                      12. Joint Displaced\n\
                                      13. Infiltration\n\
                                      14. No Defect'

mlflow_experiment = mlflow.get_experiment_by_name("/AI-ML/Sewer CCTV/sewer-cctv-image-classifier")

if mlflow_experiment == None:
    experiment_id = mlflow.create_experiment("/AI-ML/Sewer CCTV/sewer-cctv-image-classifier")
else:
    experiment_id = mlflow_experiment.experiment_id
    
    
with mlflow.start_run(run_name="sewer-cctv-image-classifier", experiment_id=experiment_id, description=_MODEL_DESCRIPTION):
    mlflow.set_tag("Release Version", "1.0")
    mlflow.set_tag("Application", "Sewer CCTV Application")
    model_details = mlflow.pyfunc.log_model(_MODEL_NAME, 
                                            python_model=wrappedModel,  
                                            conda_env=conda_environment,
                                            registered_model_name=_MODEL_NAME,
                                            artifacts=artifacts_global,
                                            signature=signature
                                           )

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

model_details = client.get_latest_versions(name=_MODEL_NAME, stages=["None"])

client.update_model_version(
    name=_MODEL_NAME,
    version=model_details[0].version,
    description=_MODEL_DESCRIPTION
)

client.update_registered_model(
    name=_MODEL_NAME,
    description=_MODEL_DESCRIPTION
)

client.set_registered_model_tag(
  name=_MODEL_NAME,
  key="Application",
  value="Sewer CCTV Application"
)

client.set_model_version_tag(
  name=_MODEL_NAME,
  version=model_details[0].version,
  key="Application",
  value="Sewer CCTV Application"
)

# COMMAND ----------

client.transition_model_version_stage(
    name=_MODEL_NAME,
    version=model_details[0].version,
    stage='Production'
)

# COMMAND ----------


