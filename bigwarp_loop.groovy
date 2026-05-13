// -------------------------------------------------------------
// Batch apply BigWarp landmarks into 2P anatomical space
//
// This version is fish-aware and writes outputs using filenames
// expected by notebooks/singleFish.ipynb cache conventions.
//
// User inputs:
//   - FISH_ID
//   - OWNER
// -------------------------------------------------------------

import java.io.File
import java.io.IOException
import java.io.FilenameFilter
import java.util.Arrays
import java.util.Locale
import groovy.io.FileType

import bdv.ij.ApplyBigwarpPlugin
import bdv.gui.TransformTypeSelectDialog
import bdv.viewer.Interpolation
import bigwarp.landmarks.LandmarkTableModel

import ij.IJ
import ij.ImagePlus
import ij.io.FileSaver
import ij.process.ImageConverter
import ij.process.StackConverter

// ---------------- USER SETTINGS ----------------

String FISH_ID = "L396_f03"
String OWNER = "Matilde"

boolean INCLUDE_BEST_ROUND_RAW_LABELS = true
boolean INCLUDE_BEST_ROUND_RAW_INTENSITY = false
boolean INCLUDE_GCAMP_CHANNEL1 = true
boolean FORCE_OVERWRITE = false
boolean DRY_RUN = false

int N_THREADS = 4
boolean IS_VIRTUAL = false   // MUST stay false for saving
boolean SAVE_INTENSITY_AS_TIFF = true

List<String> NAS_ROOT_CANDIDATES = [
        "/Volumes/jlarsch/default/D2c/07_Data",
        "/nas/FAC/FBM/CIG/jlarsch/default/D2c/07_Data",
        "\\\\nasdcsr.unil.ch\\RECHERCHE\\FAC\\FBM\\CIG\\jlarsch\\default\\D2c\\07_Data",
        "//nasdcsr.unil.ch/RECHERCHE/FAC/FBM/CIG/jlarsch/default/D2c/07_Data",
]

// ------------------------------------------------

File resolveFishDir(String fishId, String owner, List<String> rootCandidates) {
    for (String rootStr : rootCandidates) {
        File root = new File(rootStr)
        if (!root.exists()) continue

        File ownerMicroscopy = new File(root, "${owner}/Microscopy")
        File ownerRoot = new File(root, owner)

        File cand1 = new File(ownerMicroscopy, fishId)
        if (cand1.exists()) return cand1

        File cand2 = new File(ownerRoot, fishId)
        if (cand2.exists()) return cand2
    }
    return null
}

int parseRoundFromName(String name) {
    def m = (name =~ /(?i)round(\d+)/)
    if (m.find()) {
        try {
            return Integer.parseInt(m.group(1))
        } catch (Exception ignored) {}
    }
    return -1
}

String lower(String s) {
    return s == null ? "" : s.toLowerCase(Locale.ROOT)
}

String stripExtension(String filename) {
    String low = lower(filename)
    if (low.endsWith(".nii.gz")) {
        return filename.substring(0, filename.length() - 7)
    }
    int dot = filename.lastIndexOf('.')
    return (dot > 0) ? filename.substring(0, dot) : filename
}

boolean isSupportedImageFile(File f) {
    if (f == null || !f.isFile()) return false
    String n = lower(f.getName())
    return n.endsWith(".tif") || n.endsWith(".tiff") || n.endsWith(".nrrd") || n.endsWith(".nii") || n.endsWith(".nii.gz")
}

boolean isLabelLikeName(String nameLower) {
    return nameLower.contains("label") ||
           nameLower.contains("mask") ||
           nameLower.contains("seg") ||
           nameLower.contains("cellpose")
}

boolean saveAsTiffAuto(ImagePlus ip, File outFile) {
    def saver = new FileSaver(ip)
    if (ip.getStackSize() > 1) {
        return saver.saveAsTiffStack(outFile.getAbsolutePath())
    }
    return saver.saveAsTiff(outFile.getAbsolutePath())
}

boolean saveAsNrrd(ImagePlus ip, File outFile) {
    try {
        IJ.saveAs(ip, "Nrrd", outFile.getAbsolutePath())
        return outFile.exists() && outFile.length() > 0
    } catch (Exception e) {
        IJ.log("[WARN] NRRD save failed for " + outFile.getAbsolutePath() + " -> " + e.getMessage())
        return false
    }
}

void ensureLabel16(ImagePlus ip) {
    try {
        if (ip.getBitDepth() == 16) return
        if (ip.getStackSize() > 1) {
            new StackConverter(ip).convertToGray16()
        } else {
            new ImageConverter(ip).convertToGray16()
        }
    } catch (Exception e) {
        IJ.log("[WARN] Could not cast label image to 16-bit: " + e.getMessage())
    }
}

String makeLabelOutputName(File inFile) {
    String name = inFile.getName()
    String low = lower(name)

    if (low.endsWith("_in_rbest_labels_uint16.tif")) {
        return name.replaceFirst(/(?i)_in_rbest_labels_uint16\.tif$/, "_in_2p_labels_uint16.tif")
    }
    if (low.endsWith("_in_rbest_labels.tif")) {
        return name.replaceFirst(/(?i)_in_rbest_labels\.tif$/, "_in_2p_labels_uint16.tif")
    }

    String stem = stripExtension(name)
    return stem + "_in_2p_labels_uint16.tif"
}

String makeIntensityOutputName(File inFile) {
    String name = inFile.getName()
    String low = lower(name)

    if (low.endsWith("_in_rbest.nrrd")) {
        if (SAVE_INTENSITY_AS_TIFF) {
            return name.replaceFirst(/(?i)_in_rbest\.nrrd$/, "_in_2p.tif")
        }
        return name.replaceFirst(/(?i)_in_rbest\.nrrd$/, "_in_2p.nrrd")
    }
    if (low.endsWith("_in_rbest.tif")) {
        return name.replaceFirst(/(?i)_in_rbest\.tif$/, "_in_2p.tif")
    }
    if (low.endsWith("_in_rbest.tiff")) {
        return name.replaceFirst(/(?i)_in_rbest\.tiff$/, "_in_2p.tiff")
    }

    String stem = stripExtension(name)
    return stem + (SAVE_INTENSITY_AS_TIFF ? "_in_2p.tif" : "_in_2p.nrrd")
}

List<File> dedupeFiles(List<File> files) {
    LinkedHashMap<String, File> map = new LinkedHashMap<>()
    for (File f : files) {
        if (f == null) continue
        map.put(f.getAbsolutePath(), f)
    }
    return new ArrayList<>(map.values())
}

File fishDir = resolveFishDir(FISH_ID, OWNER, NAS_ROOT_CANDIDATES)
if (fishDir == null || !fishDir.exists()) {
    throw new RuntimeException("Could not resolve fish directory for ${OWNER}/${FISH_ID} under: ${NAS_ROOT_CANDIDATES}")
}

File preprocDir = new File(fishDir, "02_reg/00_preprocessing")
File rbestDir = new File(preprocDir, "rbest")
File anatDir = new File(preprocDir, "2p_anatomy")

File rnToRbestAlignedDir = new File(fishDir, "03_analysis/confocal/rn_to_rbest")
File rnToRbestLabelsDir = new File(rnToRbestAlignedDir, "labels")
File rnToRbestIntensityDir = new File(rnToRbestAlignedDir, "intensity")

File rawCpMasksDir = new File(fishDir, "03_analysis/confocal/raw/cp_masks")
File outputAlignedDir = new File(fishDir, "03_analysis/confocal/aligned")
if (!outputAlignedDir.exists()) outputAlignedDir.mkdirs()

// Resolve 2P target image
List<File> targetCandidates = [
        new File(anatDir, "${FISH_ID}_anatomy_2P_GCaMP.nrrd"),
        new File(anatDir, "${FISH_ID}_anatomy_2P_GCaMP.tif"),
        new File(anatDir, "${FISH_ID}_anatomy_2P_GCaMP.tiff"),
]
File targetFile = targetCandidates.find { it.exists() }
if (targetFile == null && anatDir.exists()) {
    File[] anyTargets = anatDir.listFiles({ d, n ->
        String low = lower(n)
        return low.contains("anatomy") && low.contains("2p") && (low.endsWith(".nrrd") || low.endsWith(".tif") || low.endsWith(".tiff"))
    } as FilenameFilter)
    if (anyTargets != null && anyTargets.length > 0) {
        Arrays.sort(anyTargets)
        targetFile = anyTargets[0]
    }
}
if (targetFile == null || !targetFile.exists()) {
    throw new RuntimeException("Could not locate 2P anatomy target image in: " + anatDir.getAbsolutePath())
}

// Resolve landmarks in rbest
File landmarksFile = null
if (rbestDir.exists()) {
    File[] lms = rbestDir.listFiles({ d, n -> lower(n).endsWith("_landmarks.csv") } as FilenameFilter)
    if (lms != null && lms.length > 0) {
        Arrays.sort(lms)
        landmarksFile = lms[0]
    }
}
if (landmarksFile == null || !landmarksFile.exists()) {
    throw new RuntimeException("Could not locate BigWarp landmarks CSV in: " + rbestDir.getAbsolutePath())
}

int bestRound = parseRoundFromName(landmarksFile.getName())
if (bestRound < 1) {
    bestRound = 1
}

IJ.log("[INFO] fishDir: " + fishDir.getAbsolutePath())
IJ.log("[INFO] targetFile: " + targetFile.getAbsolutePath())
IJ.log("[INFO] landmarksFile: " + landmarksFile.getAbsolutePath())
IJ.log("[INFO] bestRound inferred from landmarks: r" + bestRound)
IJ.log("[INFO] outputAlignedDir: " + outputAlignedDir.getAbsolutePath())

// Build input lists
List<File> labelInputs = []
List<File> intensityInputs = []

if (rnToRbestLabelsDir.exists()) {
    rnToRbestLabelsDir.eachFile(FileType.FILES) { File f ->
        String n = lower(f.getName())
        if (!isSupportedImageFile(f)) return
        if (n.contains("_in_rbest_labels")) {
            labelInputs << f
        }
    }
}

if (INCLUDE_BEST_ROUND_RAW_LABELS && rawCpMasksDir.exists()) {
    rawCpMasksDir.eachFile(FileType.FILES) { File f ->
        String n = lower(f.getName())
        if (!isSupportedImageFile(f)) return
        if (!n.contains("_cp_masks")) return
        if (!n.contains("${lower(FISH_ID)}_round${bestRound}_")) return
        labelInputs << f
    }
}

if (rnToRbestIntensityDir.exists()) {
    rnToRbestIntensityDir.eachFile(FileType.FILES) { File f ->
        String n = lower(f.getName())
        if (!isSupportedImageFile(f)) return
        if (n.contains("_in_rbest")) {
            intensityInputs << f
        }
    }
}

if (INCLUDE_BEST_ROUND_RAW_INTENSITY && rbestDir.exists()) {
    rbestDir.eachFile(FileType.FILES) { File f ->
        String n = lower(f.getName())
        if (!isSupportedImageFile(f)) return
        if (!n.contains("${lower(FISH_ID)}_round${bestRound}_channel")) return
        if (!INCLUDE_GCAMP_CHANNEL1 && n.contains("channel1") && n.contains("gcamp")) return
        if (n.contains("_landmarks")) return
        intensityInputs << f
    }
}

labelInputs = dedupeFiles(labelInputs)
intensityInputs = dedupeFiles(intensityInputs)

IJ.log("[INFO] label inputs: " + labelInputs.size())
labelInputs.each { IJ.log("  [label] " + it.getAbsolutePath()) }
IJ.log("[INFO] intensity inputs: " + intensityInputs.size())
intensityInputs.each { IJ.log("  [intensity] " + it.getAbsolutePath()) }

if (labelInputs.isEmpty() && intensityInputs.isEmpty()) {
    throw new RuntimeException("No inputs found to transform.")
}

ImagePlus targetIp = IJ.openImage(targetFile.getAbsolutePath())
if (targetIp == null) {
    throw new RuntimeException("Could not open target image: " + targetFile.getAbsolutePath())
}

int nd = (targetIp.getNSlices() > 1) ? 3 : 2
LandmarkTableModel ltm = new LandmarkTableModel(nd)
try {
    ltm.load(landmarksFile)
} catch (IOException e) {
    targetIp.close()
    throw new RuntimeException("Failed to load landmarks CSV: " + landmarksFile.getAbsolutePath())
}

class WorkItem {
    File inFile
    boolean isLabel
}

List<WorkItem> jobs = []
labelInputs.each { File f -> jobs << new WorkItem(inFile: f, isLabel: true) }
intensityInputs.each { File f -> jobs << new WorkItem(inFile: f, isLabel: false) }

int nDone = 0
int nSkip = 0
int nFail = 0

for (WorkItem job : jobs) {
    File inFile = job.inFile
    boolean isLabel = job.isLabel

    String outName = isLabel ? makeLabelOutputName(inFile) : makeIntensityOutputName(inFile)
    File outFile = new File(outputAlignedDir, outName)

    if (outFile.exists() && !FORCE_OVERWRITE) {
        IJ.log("[SKIP] exists: " + outFile.getAbsolutePath())
        nSkip++
        continue
    }

    IJ.log("[RUN] " + inFile.getName() + " -> " + outFile.getName() + (isLabel ? " [label]" : " [intensity]"))
    if (DRY_RUN) {
        nDone++
        continue
    }

    ImagePlus movingIp = IJ.openImage(inFile.getAbsolutePath())
    if (movingIp == null) {
        IJ.log("[FAIL] could not open input: " + inFile.getAbsolutePath())
        nFail++
        continue
    }

    Interpolation interp = isLabel ? Interpolation.NEARESTNEIGHBOR : Interpolation.NLINEAR

    def warpedList = ApplyBigwarpPlugin.apply(
            movingIp,
            targetIp,
            ltm,
            TransformTypeSelectDialog.TPS,
            ApplyBigwarpPlugin.TARGET,
            "",
            ApplyBigwarpPlugin.TARGET,
            null, null, null,
            interp,
            IS_VIRTUAL,
            true,
            N_THREADS
    )

    if (warpedList == null || warpedList.isEmpty()) {
        IJ.log("[FAIL] transform failed: " + inFile.getName())
        movingIp.close()
        nFail++
        continue
    }

    ImagePlus warpedIp = warpedList.get(0)
    if (warpedIp == null) {
        IJ.log("[FAIL] null warped output: " + inFile.getName())
        movingIp.close()
        nFail++
        continue
    }

    boolean ok = false
    if (isLabel) {
        ensureLabel16(warpedIp)
        ok = saveAsTiffAuto(warpedIp, outFile)
    } else {
        if (SAVE_INTENSITY_AS_TIFF) {
            ok = saveAsTiffAuto(warpedIp, outFile)
        } else {
            ok = saveAsNrrd(warpedIp, outFile)
        }
    }

    if (!ok) {
        IJ.log("[FAIL] save failed: " + outFile.getAbsolutePath())
        nFail++
    } else {
        nDone++
    }

    warpedIp.close()
    movingIp.close()
}

targetIp.close()

IJ.log("[DONE] transformed=" + nDone + " skipped=" + nSkip + " failed=" + nFail)
