import os
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from utils import get_twitter_score, generate_proof

from pydantic import BaseModel

import uvicorn

import pandas as pd 

app = FastAPI()

# middleware
app.add_middleware(
    CORSMiddleware, 
    allow_credentials=True, 
    allow_origins=["*"], 
    allow_methods=["*"], 
    allow_headers=["*"]
)

class single_respond(BaseModel):
    user_name : str

@app.post("/score_respond/")
async def single_respond(item: single_respond):
    features = get_twitter_score(item.user_name)
    print(features)
    respond = generate_proof(features)
    return {"code":0, "data":respond}

    

if __name__ == "__main__":
    
    uvicorn.run(app, host="0.0.0.0", port=8000)