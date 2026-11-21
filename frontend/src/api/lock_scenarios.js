import {api} from './client.js';
export const lockScenariosApi={list:()=>api('/lock_scenarios'),create:data=>api('/lock_scenarios','POST',data),update:data=>api('/lock_scenarios/'+data.id,'PUT',data)};
