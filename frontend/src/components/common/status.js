import {STATE_LABELS} from '../../types/enums.js';
const EXTRA={integrity:'留档校验未通过'};
export const status=x=>`<span class="status ${x}">${STATE_LABELS[x]||EXTRA[x]||x}</span>`;
