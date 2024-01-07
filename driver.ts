import * as net from "net";
import crypto from "crypto";
import {PostgresTypes} from "./types.ts";

export type TDBConfig = {
    host: string,
    port: number,
    user: string,
    password: string,
    database: string
}

type TResult = {
    fields: Array<string>,
    types: Array<string>,
    rows: Record<string, unknown>[],
    status: string
}

type TPostgresError = {

}

type TVoidFunc = (...params: any) => void




export class CustomDriver {
    private dbConfig: TDBConfig;
    private readonly results: TResult;
    private data: Buffer;

    private queryQueue: Array<any> = [];
    private socket: net.Socket | null = null;
    private callback: TVoidFunc | null = null;
    private readyToAcceptQuery: boolean = false;

    constructor(dbConfig: TDBConfig) {
        this.dbConfig = dbConfig;
        this.results = {
            fields: [],
            types: [],
            rows: [],
            status: ""
        };
        this.data = new Buffer("", "utf-8")
    }

    connect(callback: TVoidFunc) {
            const port = this.dbConfig?.port;
            const host = this.dbConfig?.host;
            this.queryQueue.push({ "callback": callback });
            this.socket = new net.Socket();
            this.addListeners();
            this.socket.connect(port!, host!);
    }

    addListeners() {
        this.socket!.on("connect", (err: any) => {
            const next = this.queryQueue.shift();
            if (err) {
                next.callback(err);
                return;
            }
            next.callback(null, "CONNECTION ESTABLISHED TO POSTGRESQL");
            this.startup();
        });
        this.socket!.on("data", (data) => { this.queryParser(data) });
        this.socket!.on("error", (err) => {
            console.log("CONNECTION ENDED");
        });

        this.socket!.on("readyForQuery", () => {
            if (this.queryQueue.length > 0) {
                this.readyToAcceptQuery = false;
                const next = this.queryQueue.shift();
                this.socket!.write(next.buffer);
                this.callback = next.callback;
            } else {
                this.readyToAcceptQuery = true;
            }
        });
    }

    startup() {
        const user = (this.dbConfig?.user)!;
        const db = (this.dbConfig?.database)!;
        const params = `user\0${user}\0database\0${db}\0\0`;
        const buffer = Buffer.alloc(4 + 4 + params.length);
        buffer.writeInt32BE(buffer.length);
        buffer.writeInt32BE(196608, 4);
        buffer.write(params, 8);
        if(this.socket) {
            this.socket.write(buffer);
        }
    }

    queryParser(data: Buffer) {
        this.data = data;
        let type: string
        let length: number
        let offset = 0;
        do {
            type = String.fromCharCode(data[offset++]);
            length = data.readInt32BE(offset);
            offset += 4;
            switch (type) {
                case 'R': {
                    this.password(offset);
                    break;
                }
                case 'E': {
                    this.error(offset, length);
                    break;
                }
                case 'Z': {
                    this.socket!.emit('readyForQuery');
                    break;
                }
                case 'T': {
                    this.fields(offset);
                    break;
                }
                case 'D': {
                    this.rows(offset);
                    break;
                }
                case 'C': {
                    const i = data.indexOf(0, offset);
                    this.results!.status = data.toString('utf-8', offset, i);
                    this.callback!(null, this.results);
                    break;
                }
            }
            offset += (length - 4);
        } while (offset < data.length);
    }

    password(offset: number) {
        let start = offset;
        const passwordType = this.data?.readInt32BE(start)!;
        start += 4;
        if (passwordType === 0) {
            return;
        } else if (passwordType === 5) {
            const salt = this.data?.slice(start, start + 4)!;
            const str = this.dbConfig?.password! + this.dbConfig?.user!;
            let md5 = crypto.createHash('md5');
            const inner = md5.update(str,  'utf-8').digest('hex');
            const buff = Buffer.concat([Buffer.from(inner), salt]);
            md5 = crypto.createHash('md5');
            const pwd = 'md5' + md5.update(buff.toString(), 'utf-8').digest('hex');
            const length = 4 + pwd.length + 1;
            const buffer = Buffer.alloc(1 + length);
            buffer.write('p');
            buffer.writeInt32BE(length, 1);
            buffer.write(pwd + '\0', 5);
            this.socket!.write(buffer);
        }
    }

    query(text: string, callback: TVoidFunc, detailed: boolean = false) {
        const length = 4 + text.length + 1;
        const buffer = Buffer.alloc(1 + length);

        buffer.write('Q');
        buffer.writeInt32BE(length, 1);
        buffer.write(text + '\0', 5);

        if (this.readyToAcceptQuery && this.queryQueue.length === 0) {
            this.readyToAcceptQuery = false;
            this.callback = callback;
            this.socket!.write(buffer);
            console.log(buffer.toString())
        } else {
            this.queryQueue.push({
                "buffer": buffer,
                "callback": callback
            });
        }
    }

    error(offset: number, length: number) {
        let start = offset, err = {},
            fieldType,
            field,
            t,
            end;

        const errorFields = {
            "C": "code",
            "M": "message",
            "S": "severity"
        };
        while (start < length) {
            fieldType = String.fromCharCode(this.data![start]);
            start++;
            if (fieldType == "0") {
                continue;
            }
            end = this.data!.indexOf(0, start);
            field = this.data!.toString("utf-8", start, end);
            //@ts-ignore
            t = errorFields[fieldType];
            if (t) {
                //@ts-ignore
                err[t] = field;
            }
            start = end + 1;
        }
        //@ts-ignore
        if (err.severity === "ERROR" || err.severity === "FATAL") {
            console.log(err)
            // this.callback(err); // notify the client about the error
        }
    }

    fields(offset: number) {
        let start = offset
        let fieldName: string;
        let end: number;
        let oid_type: number;
        const nFields = this.data!.readInt16BE(start);
        start += 2;
        if (nFields === 0) return;
        for (let i = 0; i < nFields; i++) {
            end = this.data!.indexOf(0, start);
            fieldName = this.data!.toString("utf-8", start, end);
            this.results!.fields.push(fieldName);
            start = end + 1;
            start += (4 + 2);
            oid_type = this.data!.readInt32BE(start);
            this.results!.types.push(PostgresTypes.get(oid_type.toString()));
            start += (4 + 2 + 4 + 2);
        }
    }

    rows(offset: number) {
        let start = offset, row = {}, len, val_col, name_col;
        const nColumns = this.data?.readInt16BE(start);
        start += 2;

        if (nColumns === 0) return;
        //@ts-ignore
        for (let i = 0; i < nColumns; i++) {
            name_col = this.results!.fields[i];
            len = this.data?.readInt32BE(start);
            start += 4;
            if (len === -1) {
                //@ts-ignore
                row[name_col] = null;
            } else {

                val_col = this.data!.toString('utf-8', start, start + len);
                //@ts-ignore
                row[name_col] = val_col;
                start += len;
            }
        }
        this.results!.rows.push(row);
    }

    close() {
        const buffer = Buffer.alloc(1 + 4);
        buffer.write('X');
        buffer.writeInt32BE(4, 1);
        this.socket!.end(buffer);
    }
}
