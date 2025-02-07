import AsyncLock from 'async-lock'
import { mkdir, readFile, stat, unlink, writeFile } from 'fs/promises';
import { GroupMetadata } from '../Types';
import { join } from 'path'
import { proto } from '../../WAProto'
import { AuthenticationState } from '../Types'
import { jidNormalizedUser } from '../WABinary';

const fileLock = new AsyncLock({ maxPending: Infinity })



export const SaveMetaCache = async (authState: AuthenticationState, jid: string, MetaDados: GroupMetadata) => {
    const folder = 'MetaCache/' + (jidNormalizedUser(authState.creds.me?.id)  as string);
    const file = 'groups.json';
    const fixFileName = (file?: string) => file?.replace(/\//g, '__')?.replace(/:/g, '-');

    const filePath = join(folder, fixFileName(file)!);

    try {
        await fileLock.acquire(filePath, async () => {
            try {
               
                await mkdir(folder, { recursive: true });

                let existingData: Record<string, GroupMetadata> = {};

               
                try {
                    const data = await readFile(filePath, { encoding: 'utf-8' });
                    existingData = JSON.parse(data);
                } catch (readErr) {
                    if (readErr.code !== 'ENOENT') {
                       
                        return;
                    }
                } 
				            
                const isUpdate = !!existingData[jid];
                existingData[jid] = MetaDados;

               
                await writeFile(filePath, JSON.stringify(existingData, null, 2), { encoding: 'utf-8' });

              

            } catch (error) {
                return false;
            }
        });
    } catch (lockError) {
        return false;
    }
};

export const GetMetaCache = async (authState: AuthenticationState, jid: string) => {
    const folder = 'MetaCache/' + (jidNormalizedUser(authState.creds.me?.id)  as string);
    const file = 'groups.json';
    const fixFileName = (file?: string) => file?.replace(/\//g, '__')?.replace(/:/g, '-');

    try {
        const filePath = join(folder, fixFileName(file)!);
        const data = await fileLock.acquire(
            filePath,
            async () => await readFile(filePath, { encoding: 'utf-8' })
        );

        const metaData = JSON.parse(data);

        if (metaData.hasOwnProperty(jid)) {
            return metaData[jid] as GroupMetadata;
        }
    } catch (error) {
      
        return null;
    }
};

export const ExportMetaCache = async (authState: AuthenticationState) => {
    const folder = 'MetaCache/' + (jidNormalizedUser(authState.creds.me?.id) as string);
    const file = 'groups.json';
    const fixFileName = (file?: string) => file?.replace(/\//g, '__')?.replace(/:/g, '-');

    try {
        const filePath = join(folder, fixFileName(file)!);
        const data = await fileLock.acquire(
            filePath,
            async () => await readFile(filePath, { encoding: 'utf-8' })
        );

        return JSON.parse(data); 
    } catch (error) {
        console.error('Erro ao exportar meta cache:', error);
        return null; 
    }
};


