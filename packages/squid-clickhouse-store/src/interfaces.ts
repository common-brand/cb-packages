import { HashAndHeight } from "@subsquid/util-internal-processor-tools";

export interface DatabaseState {
  height: number;
  hash: string;
  top: HashAndHeight[];
}
