import { Modal, Progress } from "antd";
import { remapProgress } from "../logic/MinecraftApi";
import { useObservable } from "../utils/UseObservable";
import {downloadProgress} from "../logic/JarProvider.ts";

const ProgressModal = () => {
    const download = useObservable(downloadProgress);
    const remap = useObservable(remapProgress);
    const isRemapping = remap !== undefined;
    const progress = isRemapping ? remap : download;

    return (
        <Modal
            title={isRemapping ? "Remapping Minecraft Jar" : "Downloading Minecraft Jar"}
            open={progress !== undefined}
            footer={null}
            closable={false}
        >
            <Progress percent={progress ?? 0} />
        </Modal>
    );
};

export default ProgressModal;
