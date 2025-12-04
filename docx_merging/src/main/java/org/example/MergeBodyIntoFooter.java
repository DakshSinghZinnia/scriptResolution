package org.example;

import org.docx4j.TraversalUtil;
import org.docx4j.XmlUtils;
import org.docx4j.jaxb.Context;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.openpackaging.parts.Part;
import org.docx4j.openpackaging.parts.WordprocessingML.BinaryPartAbstractImage;
import org.docx4j.openpackaging.parts.WordprocessingML.FooterPart;
import org.docx4j.openpackaging.parts.WordprocessingML.HeaderPart;
import org.docx4j.openpackaging.parts.WordprocessingML.MainDocumentPart;
import org.docx4j.openpackaging.parts.WordprocessingML.NumberingDefinitionsPart;
import org.docx4j.openpackaging.parts.WordprocessingML.StyleDefinitionsPart;
import org.docx4j.relationships.Relationship;
import org.docx4j.wml.*;
import org.docx4j.dml.CTBlip;

import java.io.File;
import java.math.BigInteger;
import java.util.*;

public class MergeBodyIntoFooter {

    private static final ObjectFactory WML = Context.getWmlObjectFactory();

    public static void main(String[] args) throws Exception {
        // --- Mac Downloads paths ---
        File downloads = new File(System.getProperty("user.home"), "Downloads");
        File srcHeaderFile = new File(downloads, "srcHeader.docx");
        File srcFooterFile = new File(downloads, "srcFooter.docx");
        File destFile = new File(downloads, "dest.docx");
        File outFile  = new File(downloads, "dest-with-header-footer.docx");

        if (!srcHeaderFile.exists()) throw new IllegalArgumentException("Not found: " + srcHeaderFile.getAbsolutePath());
        if (!srcFooterFile.exists()) throw new IllegalArgumentException("Not found: " + srcFooterFile.getAbsolutePath());
        if (!destFile.exists()) throw new IllegalArgumentException("Not found: " + destFile.getAbsolutePath());

        WordprocessingMLPackage dest = WordprocessingMLPackage.load(destFile);
        WordprocessingMLPackage srcHeader = WordprocessingMLPackage.load(srcHeaderFile);
        WordprocessingMLPackage srcFooter = WordprocessingMLPackage.load(srcFooterFile);

        MainDocumentPart destMain = dest.getMainDocumentPart();
        MainDocumentPart srcHeaderMain = srcHeader.getMainDocumentPart();
        MainDocumentPart srcFooterMain = srcFooter.getMainDocumentPart();

        Body body = destMain.getJaxbElement().getBody();
        if (body.getSectPr() == null) body.setSectPr(WML.createSectPr());
        SectPr sectPr = body.getSectPr();

        // 1) Create header and attach (DEFAULT header)
        HeaderPart header = new HeaderPart();
        header.setJaxbElement(WML.createHdr());
        Relationship hRel = destMain.addTargetPart(header);

        HeaderReference hRef = WML.createHeaderReference();
        hRef.setType(HdrFtrRef.DEFAULT);  // or FIRST/EVEN
        hRef.setId(hRel.getId());
        sectPr.getEGHdrFtrReferences().add(hRef);

        // 2) Create footer and attach (DEFAULT footer)
        FooterPart footer = new FooterPart();
        footer.setJaxbElement(WML.createFtr());
        Relationship fRel = destMain.addTargetPart(footer);

        FooterReference fRef = WML.createFooterReference();
        fRef.setType(HdrFtrRef.DEFAULT);  // or FIRST/EVEN
        fRef.setId(fRel.getId());
        sectPr.getEGHdrFtrReferences().add(fRef);

        // 3) Merge styles from both header and footer sources
        mergeStyles(destMain, srcHeaderMain);
        mergeStyles(destMain, srcFooterMain);

        // 4) Merge numbering from both header and footer sources
        mergeNumbering(destMain, srcHeaderMain);
        mergeNumbering(destMain, srcFooterMain);

        // 5) Copy BODY blocks from srcHeader into header
        for (Object o : srcHeaderMain.getContent()) {
            Object u = XmlUtils.unwrap(o);
            if (u instanceof P || u instanceof Tbl) {
                header.getContent().add(XmlUtils.deepCopy(u));
            }
        }

        // 6) Copy BODY blocks from srcFooter into footer
        for (Object o : srcFooterMain.getContent()) {
            Object u = XmlUtils.unwrap(o);
            if (u instanceof P || u instanceof Tbl) {
                footer.getContent().add(XmlUtils.deepCopy(u));
            }
        }

        // 7) Re-link images in copied header content
        relinkImages(srcHeaderMain, header, dest);

        // 8) Re-link images in copied footer content
        relinkImages(srcFooterMain, footer, dest);

        dest.save(outFile);
        System.out.println("Saved: " + outFile.getAbsolutePath());
    }

    // ---------- Helpers ----------

    private static void mergeStyles(MainDocumentPart destMain, MainDocumentPart srcMain) throws Exception {
        StyleDefinitionsPart destStylesPart = destMain.getStyleDefinitionsPart();
        if (destStylesPart == null) {
            destStylesPart = new StyleDefinitionsPart();
            destMain.addTargetPart(destStylesPart);
            destStylesPart.setJaxbElement(WML.createStyles());
        }
        StyleDefinitionsPart srcStylesPart = srcMain.getStyleDefinitionsPart();
        if (srcStylesPart == null) return;

        Styles destStyles = destStylesPart.getContents();
        Styles srcStyles  = srcStylesPart.getContents();

        Set<String> destIds = new HashSet<>();
        for (Style s : destStyles.getStyle()) {
            destIds.add(s.getStyleId());
        }
        for (Style s : srcStyles.getStyle()) {
            if (s.getStyleId() != null && !destIds.contains(s.getStyleId())) {
                destStyles.getStyle().add(XmlUtils.deepCopy(s));
            }
        }
    }

    private static void mergeNumbering(MainDocumentPart destMain, MainDocumentPart srcMain) throws Exception {
        NumberingDefinitionsPart srcNdp = srcMain.getNumberingDefinitionsPart();
        if (srcNdp == null) return;

        NumberingDefinitionsPart destNdp = destMain.getNumberingDefinitionsPart();
        if (destNdp == null) {
            destNdp = new NumberingDefinitionsPart();
            destMain.addTargetPart(destNdp);
            destNdp.setJaxbElement(WML.createNumbering());
        }

        Numbering destNum = destNdp.getJaxbElement();
        Numbering srcNum  = srcNdp.getJaxbElement();

        long maxAbs = 0L;
        if (destNum.getAbstractNum() != null) {
            for (Numbering.AbstractNum a : destNum.getAbstractNum()) {
                if (a.getAbstractNumId() != null) {
                    long v = a.getAbstractNumId().longValue();
                    if (v > maxAbs) maxAbs = v;
                }
            }
        }
        long maxNum = 0L;
        if (destNum.getNum() != null) {
            for (Numbering.Num n : destNum.getNum()) {
                if (n.getNumId() != null) {
                    long v = n.getNumId().longValue();
                    if (v > maxNum) maxNum = v;
                }
            }
        }

        Map<Long, Long> absIdMap = new HashMap<>();

        // copy AbstractNum (attribute abstractNumId)
        if (srcNum.getAbstractNum() != null) {
            for (Numbering.AbstractNum a : srcNum.getAbstractNum()) {
                if (a.getAbstractNumId() == null) continue;
                long oldId = a.getAbstractNumId().longValue();

                long newId = ++maxAbs;
                absIdMap.put(oldId, newId);

                Numbering.AbstractNum copy = XmlUtils.deepCopy(a);
                copy.setAbstractNumId(BigInteger.valueOf(newId)); // attr in 6.1.2
                destNum.getAbstractNum().add(copy);
            }
        }

        // copy Num (attribute numId) and remap its <w:abstractNumId w:val="...">
        if (srcNum.getNum() != null) {
            for (Numbering.Num n : srcNum.getNum()) {
                long newNumId = ++maxNum;

                Numbering.Num copy = XmlUtils.deepCopy(n);
                copy.setNumId(BigInteger.valueOf(newNumId)); // attr in 6.1.2

                Numbering.Num.AbstractNumId ref = copy.getAbstractNumId();
                if (ref != null && ref.getVal() != null) {
                    long oldAbs = ref.getVal().longValue();
                    Long mapped = absIdMap.get(oldAbs);
                    if (mapped != null) {
                        ref.setVal(BigInteger.valueOf(mapped)); // <w:abstractNumId w:val="...">
                    }
                }

                destNum.getNum().add(copy);
            }
        }
    }

    private static void relinkImages(MainDocumentPart srcMain, Part targetPart, WordprocessingMLPackage destPkg) throws Exception {
        if (srcMain.getRelationshipsPart() == null) return;

        Object targetJaxbElement;
        if (targetPart instanceof HeaderPart) {
            targetJaxbElement = ((HeaderPart) targetPart).getJaxbElement();
        } else if (targetPart instanceof FooterPart) {
            targetJaxbElement = ((FooterPart) targetPart).getJaxbElement();
        } else {
            return;
        }

        List<CTBlip> blips = getAllElements(targetJaxbElement, CTBlip.class);

        for (CTBlip blip : blips) {
            String oldRid = blip.getEmbed();
            if (oldRid == null) continue;

            Relationship srcRel = srcMain.getRelationshipsPart().getRelationshipByID(oldRid);
            if (srcRel == null) continue;

            Part srcPart = srcMain.getRelationshipsPart().getPart(srcRel);
            if (!(srcPart instanceof BinaryPartAbstractImage)) continue;

            BinaryPartAbstractImage srcImg = (BinaryPartAbstractImage) srcPart;
            java.nio.ByteBuffer buffer = srcImg.getBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);

            BinaryPartAbstractImage newImg = BinaryPartAbstractImage.createImagePart(destPkg, bytes);
            Relationship newRel = targetPart.addTargetPart(newImg);
            blip.setEmbed(newRel.getId());
        }
    }

    private static <T> List<T> getAllElements(Object root, final Class<T> type) {
        final List<T> out = new ArrayList<>();
        new TraversalUtil(root, new TraversalUtil.CallbackImpl() {
            @Override
            public List<Object> apply(Object o) {
                Object u = XmlUtils.unwrap(o);
                if (type.isInstance(u)) out.add(type.cast(u));
                return null;
            }
        });
        return out;
    }
}