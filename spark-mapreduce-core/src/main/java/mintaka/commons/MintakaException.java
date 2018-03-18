/*
 * 广州丰石科技有限公司拥有本软件版权2017并保留所有权利。
 * Copyright 2017, Guangzhou Rich Stone Data Technologies Company Limited,
 * All rights reserved.
 */

package mintaka.commons;

import java.io.Serializable;

/**
 * Created by qinjiazheng on 2017/8/8.
 */
public class MintakaException extends RuntimeException implements Serializable {

    private static final long serialVersionUID = 8936200850305679638L;

    /**
     * Instantiates a new Mintaka exception.
     */
    public MintakaException() {
    }

    /**
     * Instantiates a new Mintaka exception.
     *
     * @param arg0 the arg 0
     */
    public MintakaException(String arg0) {
        super(arg0);
    }

    /**
     * Instantiates a new Mintaka exception.
     *
     * @param arg0 the arg 0
     */
    public MintakaException(Throwable arg0) {
        super(arg0);
    }

    /**
     * Instantiates a new Mintaka exception.
     *
     * @param arg0 the arg 0
     * @param arg1 the arg 1
     */
    public MintakaException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * Instantiates a new Mintaka exception.
     *
     * @param arg0 the arg 0
     * @param arg1 the arg 1
     * @param arg2 the arg 2
     * @param arg3 the arg 3
     */
    public MintakaException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
        super(arg0, arg1, arg2, arg3);
    }
}