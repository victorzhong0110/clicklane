package io.pulseboard.pipeline.model;

import java.io.Serializable;

/** 商品维表的一行。类目名来自数据集的 category_code，缺失时写成 UNKNOWN。 */
public class DimItem implements Serializable {
    public long itemId;
    public long categoryId;
    public String categoryCode = "UNKNOWN";

    public DimItem() {}

    public DimItem(long itemId, long categoryId, String categoryCode) {
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.categoryCode = categoryCode == null || categoryCode.isEmpty() ? "UNKNOWN" : categoryCode;
    }
}
