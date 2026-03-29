Repo of sam3-pipline : https://github.com/hamimmahmud0/kaggle-sam3-pipline.git


Current repo is based on notebook cell execution, It also provides a erb interface using ngrok. You need to modify this repo to work like sam3-pipline. Investigate sam3 pipeline for better understanding.


sam3-pipline
================
 - Connects to remote ssh and auto setups pipeline in the remote server.
 - mega drive integration
 - samtop and samlog for worker monitoring
 - 2 workers - one worker per gpu

Addition:
 - In sam3 pipeline we used mega drive. Now we want also incorporate fare-driver into this pipeline. The github repo for fare-drive: https://github.com/hamimmahmud0/Fare-Drive.git
 - Defaults to mega drive, user can choose to use fare-drive instead of mega drive.
 - Uses predifined config file to set up the pipeline (fare-drive configuration info, etc.)


Interpret and invstigate the provided repos from github.

Repo should include:
Create a new conda envo for execution in both local pc and remote server. (seperate envo per machine) 
Host fare-drive server in remote environment and parse login access token to client side (local pc) 
For da3-pipeline server use remote environment.
Incorparate automated setup script into the pipeline.
Build `samtop` like `datop` and `samlog` like `datalog`. 
For auth in fare-drive use access token.
Build the project and test all features.
Drive link folder for video file: https://drive.google.com/drive/folders/1SWlrL2pjpM11mYTZAQCyLZJKjwdAGY76?usp=sharing
Local machine works as a manager for remote server. Main execution of the pipeline is done in remote server.
Fare-drive is used to auto-syncronize data from remote-server to local pc. (local pc should act as client)