import hashlib,json
from pathlib import Path
import numpy as np
import pytest
import tifffile
from codeants_2pf_hcr.suite2p_provenance import check_suite2p_input_provenance,reconcile_legacy_functional_provenance
from codeants_2pf_hcr.suite2p import load_suite2p_stage,Suite2pStageConfig


def fixture(tmp_path):
    fish=tmp_path/"current/L000_f00";old=tmp_path/"gone";rawdir=fish/"01_raw/2p/functional";rawdir.mkdir(parents=True)
    rng=np.random.default_rng(123);raws=[]
    for block in range(3):
        raw=rng.integers(0,100,size=(30,8,8),dtype=np.int16)
        p=rawdir/f"block{block}.tif";tifffile.imwrite(p,raw,photometric="minisblack");raws.append((p,raw))
    individual=fish/"02_reg/00_preprocessing/2p_functional/01_individualPlanes";individual.mkdir(parents=True)
    motion=individual.parent/"02_motionCorrected";motion.mkdir()
    records=[];movies=[]
    for i in range(5):
        frames=np.array([(a[v*15+i*3+1].astype(float)+a[v*15+i*3+2])/2+20 for _,a in raws for v in range(2)]).astype(np.uint16)
        p=individual/f"L000_f00_plane{i}.tif";tifffile.imwrite(p,frames,photometric="minisblack")
        mc=motion/f"L000_f00_plane{i}_mcorrected.tif";tifffile.imwrite(mc,frames,photometric="minisblack")
        suite=fish/"03_analysis/functional/suite2P"/f"plane{i}";suite.mkdir(parents=True)
        ops={"tiff_list":[str(old/p.relative_to(fish.parent))],"nframes":6,"Ly":8,"Lx":8,"meanImg":frames.mean(axis=0)}
        np.save(suite/"ops.npy",ops);np.save(suite/"F.npy",np.ones((1,6)));np.save(suite/"Fneu.npy",np.ones((1,6)))
        np.save(suite/"spks.npy",np.ones((1,6)));np.save(suite/"iscell.npy",np.array([[1,1]]))
        np.save(suite/"stat.npy",np.array([{"ypix":np.array([1]),"xpix":np.array([1])}],dtype=object))
        records.append({"plane_index":i,"output_path":str(p),"output_xy_frame":"codeants_2p_canonical_xy_v1","xy_transform":"flipX"})
        movies.append({"plane_index":i,"output_path":str(mc),"output_xy_frame":"codeants_2p_canonical_xy_v1","xy_transform":"flipX"})
    metadata={"n_planes":5,"n_frames_per_plane":3,"remove_first_frame":True,"volume_flyback_frames":0,"negative_offset_applied":20,
              "sessions":[{"output_planes":list(range(5)),"selected_tiffs":[str(old/p.relative_to(fish.parent)) for p,_ in raws]}]}
    (individual/"L000_f00_preprocessing_metadata.json").write_text(json.dumps(metadata))
    manifest=individual.parent.parent/"spatial_preprocessing_manifest.json"
    manifest.write_text(json.dumps({"stage":"canonical_spatial_preprocessing","status":"complete","fish_id":fish.name,
        "coordinate_frames":{"canonical_functional_xy":"codeants_2p_canonical_xy_v1","canonical_anatomy_xy":"codeants_2p_canonical_xy_v1","canonical_anatomy_z":"codeants_confocal_registration_z_v1"},
        "functional_planes":records,"motion_corrected_movies":movies}))
    return fish,old,manifest


def test_reconcile_preserves_ops_and_orients_labels_once(tmp_path):
    fish,old,manifest=fixture(tmp_path);before=manifest.read_bytes()
    op=fish/"03_analysis/functional/suite2P/plane0/ops.npy";ops_before=op.read_bytes()
    with pytest.raises(ValueError,match="Undeclared"):
        check_suite2p_input_provenance(fish_dir=fish,plane_indices=(0,))
    result=reconcile_legacy_functional_provenance(fish_dir=fish,original_data_root=old)
    assert result["status"]=="pass" and len(result["planes"])==5
    assert op.read_bytes()==ops_before
    assert manifest.with_name(manifest.name+".before_suite2p_reconciliation").read_bytes()==before
    payload=json.loads(manifest.read_text())
    assert len(payload["suite2p_provenance_reconciliation"]["planes"][0]["raw_samples"])==6
    loaded=load_suite2p_stage(plane_refs=[],suite2p_root=fish/"03_analysis/functional/suite2P",polarity="south",config=Suite2pStageConfig(verbose=False))
    assert loaded["func_labels"][0][1,6]==1 and loaded["func_labels"][0][1,1]==0


def test_changed_ops_fails_closed(tmp_path):
    fish,old,_=fixture(tmp_path);reconcile_legacy_functional_provenance(fish_dir=fish,original_data_root=old)
    op=fish/"03_analysis/functional/suite2P/plane0/ops.npy"
    ops=np.load(op,allow_pickle=True).item();ops["new"]=1;np.save(op,ops)
    with pytest.raises(ValueError,match="ops hash changed"):check_suite2p_input_provenance(fish_dir=fish,plane_indices=(0,))


def test_changed_source_fails_closed(tmp_path):
    fish,old,_=fixture(tmp_path);reconcile_legacy_functional_provenance(fish_dir=fish,original_data_root=old)
    p=fish/"01_raw/2p/functional/block0.tif"
    with p.open("ab") as f:f.write(b"changed")
    with pytest.raises(ValueError,match="input changed"):check_suite2p_input_provenance(fish_dir=fish,plane_indices=(0,))


def test_wrong_orientation_rejected_before_writing(tmp_path):
    fish,old,manifest=fixture(tmp_path);before=manifest.read_bytes()
    p=fish/"02_reg/00_preprocessing/2p_functional/01_individualPlanes/L000_f00_plane0.tif"
    movie=tifffile.imread(p);tifffile.imwrite(p,movie[:,:,::-1],photometric="minisblack")
    with pytest.raises(ValueError,match="Acquisition-frame evidence failed"):
        reconcile_legacy_functional_provenance(fish_dir=fish,original_data_root=old)
    assert manifest.read_bytes()==before
    assert not manifest.with_name(manifest.name+".before_suite2p_reconciliation").exists()


def test_wrong_trace_count_rejected_before_writing(tmp_path):
    fish,old,manifest=fixture(tmp_path);before=manifest.read_bytes()
    np.save(fish/"03_analysis/functional/suite2P/plane0/F.npy",np.ones((1,5)))
    with pytest.raises(ValueError,match="before repair"):
        reconcile_legacy_functional_provenance(fish_dir=fish,original_data_root=old)
    assert manifest.read_bytes()==before


def test_metadata_change_rejects_reconciled_inputs(tmp_path):
    fish,old,_=fixture(tmp_path);reconcile_legacy_functional_provenance(fish_dir=fish,original_data_root=old)
    p=fish/"02_reg/00_preprocessing/2p_functional/01_individualPlanes/L000_f00_preprocessing_metadata.json"
    p.write_text(p.read_text()+" ")
    with pytest.raises(ValueError,match="metadata changed"):
        check_suite2p_input_provenance(fish_dir=fish,plane_indices=(0,))
