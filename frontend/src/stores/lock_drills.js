import {lockDrillsApi} from '../api/lock_drills.js';
export const lockDrillsStore={items:[],async refresh(){this.items=await lockDrillsApi.list();return this.items;}};
