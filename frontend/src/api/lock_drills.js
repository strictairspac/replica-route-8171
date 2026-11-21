import {api} from './client.js';
export const lockDrillsApi={list:()=>api('/lock_drills'),create:data=>api('/lock_drills','POST',data)};
