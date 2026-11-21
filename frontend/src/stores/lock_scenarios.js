import {lockScenariosApi} from '../api/lock_scenarios.js';
export const lockScenariosStore={items:[],async refresh(){this.items=await lockScenariosApi.list();return this.items;}};
