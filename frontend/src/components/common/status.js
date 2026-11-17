import {runStates,runPhases,reviewStates} from '../../constants/enums.js';import {esc} from '../../utils/format.js';
export function status(state,phase){const label=runPhases[phase]||runStates[state]||reviewStates[state]||state;return '<span class="status '+esc(state)+(phase?' phase-'+esc(phase):'')+'">'+esc(label)+'</span>'}
