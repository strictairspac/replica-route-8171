from fastapi import APIRouter,Depends
from src.config.database import db_session
from src.utils.security import current,writer
from src.schemas.lock import LockScenarioInput
from src.services import lock_scenarios as service
router=APIRouter()
@router.get('/lock_scenarios')
def listing(db=Depends(db_session),user=Depends(current)):return service.listing(db)
@router.post('/lock_scenarios',status_code=201)
def create(data:LockScenarioInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,data,user)
@router.put('/lock_scenarios/{ident}')
def update(ident:int,data:LockScenarioInput,db=Depends(db_session),user=Depends(writer)):return service.update(db,ident,data,user)
