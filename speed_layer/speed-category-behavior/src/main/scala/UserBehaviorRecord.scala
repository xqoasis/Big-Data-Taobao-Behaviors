import scala.reflect.runtime.universe._


case class UserBehaviorRecord(
                                        user_id: String,
                                        item_id: String,
                                        category_id: String,
                                        pv_val: Boolean,
                                        fav_val: Boolean,
                                        cart_val: Boolean,
                                        buy_val: Boolean)